import { Mongo } from 'meteor/mongo'
import { Log } from 'meteor/logging'
import { _ } from 'meteor/underscore'
import ObservableCollection from 'digitaledgeit-observable-collection'

import { Job } from './job'


export class JobQueue {
  constructor(options) {
    this._types = {}

    this._options = options || {}
    if (!this._options.collection) this._options.collection = new Mongo.Collection('job_queue')

    const _transform = this._options.collection._transform
    this._options.collection._transform = (doc) => {
      const job = _transform ? _transform(doc) : doc
      const typeClass = this.getTypeClass(doc.type)
      let jobInstance
      if (!typeClass) {
        Log.warn(
          `cannot find class for type "${doc.type}". Did you forgot calling "registerType"?`
        );
        jobInstance = Job.fromDoc(job)
      } else {
        jobInstance = typeClass.fromDoc(job)
      }
      jobInstance._collection = this._options.collection
      return jobInstance
    }
  }

  getTypeClass(name) {
    return this._types[name]
  }
  getTypeName(job) {
    for (const name of _.keys(this._types)) {
      if (job instanceof this._types[name]) {
        return name
      }
    }
    return null
  }

  registerType(name, type) {
    this._types[name] = type
  }

  enqueueJob(job) {
    let type = this.getTypeName(job)
    if (!type) {
      Log.warn(
        `cannot find type for job "${job.constructor.name}". Did you forgot calling "registerType"?`
      );
    }
    this._options.collection.insert(_.extend(job.toObject(), {
      type,
      createdAt: new Date(),
      failures: 0,
    }))
  }

  startWorker(options) {
    const opts = _.defaults(options, {
      retries: 5,
      concurrency: 1,
      retryDelay: 5000, // 5 seconds
      query: {},
    })
    const queue = new ObservableCollection()
    let running = 0

    this._options.collection.find({
      finishedAt: { $exists: false },
      running: { $ne: true },
      failures: { $lt: opts.retries },
      $and: [opts.query],
    }, {
      limit: 20,
      sort: { createdAt: 1 },
    }).observeChanges({
      addedBefore: (_id, job) => {
        queue.append(this._options.collection._transform(_.extend(job, { _id })))
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
        const job = queue.items[0]

        if (!job) {
          running--
          return
        }
        queue.removeAt(0)
        setTimeout(Meteor.bindEnvironment(() => {
          if (job.isStarted() || job.isFinished()) {
            running--
            return
          }
          this._options.collection.update({ _id: job._id }, {
            $set: { startedAt: new Date(), running: true },
            $inc: { starts: 1 },
          })
          processJob(job).then(() => {
            running--
            this._options.collection.update({ _id: job._id }, {
              $set: { finishedAt: new Date(), running: false },
            })
            resolve()
          }).catch((err) => {
            running--
            this._options.collection.update({ _id: job._id }, {
              $set: { failedAt: new Date(), running: false },
              $inc: { failures: 1 },
            })
            const j = this._options.collection.findOne({ _id: job._id })
            if (opts.onFail) opts.onFail(j, err)
            if (j.failures < opts.retries) {
              queue.append(job)
            }
            reject(err)
          })
        }), Math.floor(Math.random() * 250))
      } catch (err) {
        running--
      }
    })

    const start = () => {
      for (let i = 1; i <= (opts.concurrency - running); i++) {
        runJob().then(start)
      }
    }

    queue.on('added', start)
    start()
  }
}
