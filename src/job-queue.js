import { Mongo } from 'meteor/mongo'
import { Log } from 'meteor/logging'

import { Job } from './job'
import { Worker } from './worker'


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
      jobInstance._collectionName = this._options.collection._name
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
    return new Worker(this, options)
  }
}
