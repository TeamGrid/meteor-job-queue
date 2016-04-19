import { _ } from 'meteor/underscore'

export class Job {
  constructor(data) {
    this.data = data
  }

  toObject() {
    return {
      data: this.data,
    }
  }

  isStarted() {
    const job = this._collection.findOne({ _id: this._id })
    return !!job.running
  }

  isFinished() {
    const job = this._collection.findOne({ _id: this._id })
    return !!job.finishedAt
  }
}

Job.fromDoc = function fromDoc(doc) {
  const job = new this(doc.data)
  return _.extend(job, doc)
}
