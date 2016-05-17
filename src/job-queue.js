import { Mongo } from 'meteor/mongo'
import { Log } from 'meteor/logging'

import { Job } from './job'
import { Worker } from './worker'


export class JobQueue {
  constructor(options) {
    this._types = {}

    this._options = options || {}
    if (!this._options.collection) this._options.collection = new Mongo.Collection('job_queue')

    const _transform = this.getCollection()._transform
    this.getCollection()._transform = (doc) => {
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
      jobInstance._collectionName = this.getCollection()._name
      return jobInstance
    }
  }

  getCollection() {
    return this._options.collection
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
    const type = this.getTypeName(job)
    const collection = this.getCollection()
    if (!type) {
      Log.warn(
        `cannot find type for job "${job.constructor.name}". Did you forgot calling "registerType"?`
      );
    }

    if (job.rerunThreshold) {
      const lastJob = collection.findOne({
        $and: [job.findSimilar(), {
          type,
          doneBy: { $exists: false},
          createdAt: { $gte: new Date(Date.now() - job.rerunThreshold) },
        }]
      })
      if (lastJob) {
        collection.insert(_.extend(job.toObject(), {
          type,
          doneBy: lastJob._id,
          createdAt: new Date(),
          failures: 0,
        }))
        return
      }
    }

    const jobId = collection.insert(_.extend(job.toObject(), {
      type,
      createdAt: new Date(),
      failures: 0,
    }))
    if (job.stopSimilar) {
      const done = collection.update({
        $and: [job.findSimilar(), {
          doneBy: { $exists: false },
          finishedAt: { $exists: false },
          running: { $ne: true },
          _id: { $ne: jobId },
          type,
        }]
      }, { $set: { doneBy: jobId } }, { multi: true })
    }
  }

  startWorker(options) {
    return new Worker(this, options)
  }
}
