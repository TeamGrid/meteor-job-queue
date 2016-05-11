import { _ } from 'meteor/underscore'
import ObservableCollection from 'digitaledgeit-observable-collection'
import { EventEmitter } from 'events'

export class Worker extends EventEmitter {
  constructor(jobQueue, options) {
    super()
    const collection = (jobQueue._options && jobQueue._options.collection) || jobQueue
    const opts = _.defaults(options, {
      retries: 5,
      concurrency: 1,
      retryDelay: 5000, // 5 seconds
      query: {},
    })
    const queue = new ObservableCollection()
    let running = 0

    this._observeHandle = collection.find({
      finishedAt: { $exists: false },
      running: { $ne: true },
      failures: { $lt: opts.retries },
      $and: [opts.query],
    }, {
      limit: 20,
      sort: { createdAt: 1 },
      pollingIntervalMs: 1000,
      disableOplog: false,
    }).observeChanges({
      addedBefore: (_id, job) => {
        queue.append(collection._transform(_.extend(job, { _id })))
      },
      removed: (_id) => {
        queue.find((item, index) => {
          if (item._id === _id) {
            queue.removeAt(index)
            return true
          }
          return false
        })
      },
    })

    const processJob = (job) => new Promise((resolve, reject) => {
      const process = () => {
        try {
          job.process().then(resolve).catch(reject)
        } catch (err) {
          reject(err)
        }
      }

      if (job.failures) {
        Meteor.setTimeout(process, opts.retryDelay)
      } else {
        process()
      }
    })

    const runJob = () => new Promise((resolve, reject) => {
      try {
        running++
        const jobItem = queue.items[0]

        if (!jobItem) {
          running--
          return
        }
        queue.removeAt(0)

        const job = collection.findOne({ _id: jobItem._id })
        setTimeout(Meteor.bindEnvironment(() => {
          if (job.isStarted() || job.isFinished()) {
            running--
            return
          }
          collection.update({ _id: job._id }, {
            $set: { startedAt: new Date(), running: true },
            $inc: { starts: 1 },
          })
          this.emit('start', job)
          processJob(job).then((outcome) => {
            running--
            collection.update({ _id: job._id }, {
              $set: { finishedAt: new Date(), running: false, outcome },
            })
            this.emit('finish', collection.findOne({ _id: job._id }))
            resolve()
          }).catch((err) => {
            collection.update({ _id: job._id }, {
              $set: {
                failedAt: new Date(),
                running: false,
                stackTrace: err.stack,
              },
              $inc: { failures: 1 },
            })
            const j = collection.findOne({ _id: job._id })
            this.emit('failure', j, err)
            if (j.failures < opts.retries) {
              queue.append(job)
            }
            running--
            reject(err)
          })
        }), Math.floor(Math.random() * 250))
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
          console.log(err.stack);
          throw err
        })
      }
    }

    queue.on('added', (item) => {
      this.emit('add', item)
      start()
    })
    queue.on('removed', (item) => this.emit('remove', item))
    start()
  }

  stop() {
    if (this._observeHandle) {
      this._observeHandle.stop()
      this._stopped = true
    }
  }
}
