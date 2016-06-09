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
    this.updateProgressThrottled = _.throttle(
      this.updateProgress.bind(this), this.getUpdateTimeout())
  }

  getUpdateTimeout() {
    return this.updateTimeout || 1000
  }

  toObject() {
    const job = {
      data: this.data,
    }
    if (this.progress) {
      job.progress = this.progress
    }
    return job
  }

  isStarted() {
    const job = this._collection().findOne({ _id: this._id })
    return !!job.running
  }

  isFinished() {
    const job = this._collection().findOne({ _id: this._id })
    return !!job.finishedAt
  }

  findSimilar() {
    return { data: this.data }
  }

  process() {
    throw new Error(`process method not implemented for job type '${this.constructor.name}'`)
  }

  updateProgress() {
    this._collection().update({ _id: this._id }, {
      $set: {
        progress: this.progress,
      },
    })
  }

  setProgress(current, finish, force) {
    if (!finish) return
    this.progress = {
      current,
      finish,
      percent: Math.floor(100 / finish * current),
    }
    if (this._collection()) {
      if (force) {
        this.updateProgress()
      } else {
        this.updateProgressThrottled()
      }
    }
  }
}

Job.fromDoc = function fromDoc(doc) {
  const job = new this(doc.data)
  return _.extend(job, doc)
}
