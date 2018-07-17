import { _ } from 'meteor/underscore'
import ObservableCollection from 'digitaledgeit-observable-collection'
import { EventEmitter } from 'events'
import { Meteor } from 'meteor/meteor'

export class Worker extends EventEmitter {
  constructor(jobQueue, options) {
    super()
    this._collection = (jobQueue._options && jobQueue._options.collection) || jobQueue
    const opts = this._options = _.defaults(options, {
      retries: 5,
      concurrency: 1,
      retryDelay: 5000, // 5 seconds
      disableOplog: false,
      pollingIntervalMs: 1000,
      query: {},
      delay: () => Math.floor(Math.random() * 250),
    })

    const calcJobDelay = () => {
      if (typeof opts.delay === 'function') return opts.delay()
      return opts.delay
    }

    const queryOptions = (() => {
      const obj = {}
      if (opts.pollingIntervalMs) obj.pollingIntervalMs = opts.pollingIntervalMs
      obj.disableOplog = !!opts.disableOplog
      if (opts.queueSize) obj.limit = opts.queueSize
      obj.sort = {
        failures: 1,
        createdAt: 1,
      }
      return obj
    })()

    this._queue = new ObservableCollection()
    let running = 0
    this._runningJobIds = []

    this._observeHandle = this._collection.find({
      doneBy: { $exists: false },
      finishedAt: { $exists: false },
      running: { $ne: true },
      failures: { $lt: opts.retries },
      $and: [opts.query],
    }, queryOptions).observeChanges({
      added: (_id, job) => {
        this._queue.append(this._collection._transform(_.extend(job, { _id })))
      },
      removed: (_id) => {
        this._queue.find((item, index) => {
          if (item._id === _id) {
            this._queue.removeAt(index)
            return true
          }
          return false
        })
      },
    })

    const processJob = (job, registered, started) => new Promise((resolve, reject) => {
      try {
        const startJob = () => {
          started()
          job.process().then(resolve).catch(reject)
        }
        if (job.failures) {
          registered(true)
          Meteor.setTimeout(startJob, opts.retryDelay)
        } else {
          registered(false)
          startJob()
        }
      } catch(err) {
        reject(err)
      }
    })

    const runJob = () => new Promise((resolve, reject) => {
      try {
        running++
        const jobItem = this._queue.items[0]

        if (!jobItem) {
          running--
          return
        }
        this._queue.removeAt(0)
        this._addRunningJob(jobItem._id)

        const job = this._collection.findOne({ _id: jobItem._id })
        setTimeout(Meteor.bindEnvironment(() => {
          if (job.isStarted() || job.isFinished()) {
            this._removeRunningJob(job._id)
            running--
            return
          }
          this._collection.update({ _id: job._id }, {
            $set: {
              startedAt: new Date(),
              running: true,
              instanceId: opts.instanceId,
            },
            $inc: { starts: 1 },
          })
          this.emit('start', job)
          processJob(job, (delayed) => {
            // registered
            if (delayed) start()
            running--
          }, () => {
            // started
            running++
          }).then((outcome) => {
            this._removeRunningJob(job._id)
            running--
            this._collection.update({ _id: job._id }, {
              $set: { finishedAt: new Date(), running: false, outcome },
            })
            this.emit('finish', this._collection.findOne({ _id: job._id }))
            resolve()
          }).catch((err) => {
            this._collection.update({ _id: job._id }, {
              $set: {
                failedAt: new Date(),
                running: false,
                stackTrace: err.stack,
              },
              $inc: { failures: 1 },
            })
            const j = this._collection.findOne({ _id: job._id })
            this.emit('failure', j, err)
            if (j.failures < opts.retries) {
              this._queue.append(job)
            }
            this._removeRunningJob(j._id)
            running--
            reject(err)
          })
        }), calcJobDelay())
      } catch (err) {
        running--
        reject(err);
      }
    })

    const start = () => {
      if (this._stopped) return
      for (let i = 1; i <= (opts.concurrency - running); i++) {
        runJob().then(start).catch((err) => {
          start()
          throw err
        })
      }
    }

    this._queue.on('added', (item) => {
      this.emit('add', item)
      start()
    })
    this._queue.on('removed', (item) => this.emit('remove', item))
    start()
  }

  _addRunningJob(id) {
    this._runningJobIds.push(id)
  }

  _removeRunningJob(id) {
    const index = this._runningJobIds.indexOf(id)
    if (index > -1) this._runningJobIds.splice(index, 1)
  }

  stop(force = false) {
    if (this._observeHandle) this._observeHandle.stop()
    if (this._queue) this._queue.removeAll()

    if (force) {
      this._collection.update({
        _id: { $in: this._runningJobIds },
        running: true,
      }, {
        $set: {
          failedAt: new Date(),
          running: false,
          stackTrace: 'forced kill',
        },
        $inc: { failures: 1 },
      }, { multi: true })
    }

    this._stopped = true
  }
}
