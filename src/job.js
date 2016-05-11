import { _ } from 'meteor/underscore'
import { Mongo } from 'meteor/mongo'
import 'meteor/dburles:mongo-collection-instances'
export class Job {
  _collection() {
    if (!this._collectionName) {
      throw new Error('cannot get collection before the name was defined')
    }
    return Mongo.Collection.get(this._collectionName)
  }

  constructor(data) {
    this.data = data
  }

  toObject() {
    return {
      data: this.data,
    }
  }

  isStarted() {
    const job = this._collection().findOne({ _id: this._id })
    return !!job.running
  }

  isFinished() {
    const job = this._collection().findOne({ _id: this._id })
    return !!job.finishedAt
  }

  process() {
    throw new Error(`process method not implemented for job type '${this.constructor.name}'`)
  }
}

Job.fromDoc = function fromDoc(doc) {
  const job = new this(doc.data)
  return _.extend(job, doc)
}
