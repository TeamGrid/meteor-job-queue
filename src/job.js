import { _ } from 'meteor/underscore'
import { Mongo } from 'meteor/mongo'
import { Meteor } from 'meteor/meteor'
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
      Meteor.bindEnvironment(this.updateProgress.bind(this)), this.getUpdateTimeout())
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

  increaseProgress(value = 1, force = false) {
    if (!this.progress) this.progress = {}
    if (!this.progress.current) {
      this.progress.current = value
    } else {
      this.progress.current += value
    }

    if (this.progress.finish) {
      this.progress.percent = Math.floor(100 / this.progress.finish * (this.progress.current || 0))
    }

    if (this._collectionName) {
      if (force) {
        this.updateProgress()
      } else {
        this.updateProgressThrottled()
      }
    }
  }

  increaseFinish(value = 1, force = false) {
    if (!this.progress) this.progress = {}
    if (!this.progress.finish) {
      this.progress.finish = value
    } else {
      this.progress.finish += value
    }

    if (this.progress.finish) {
      this.progress.percent = Math.floor(100 / this.progress.finish * (this.progress.current || 0))
    }

    if (this._collectionName) {
      if (force) {
        this.updateProgress()
      } else {
        this.updateProgressThrottled()
      }
    }
  }

  setProgress(current, finish, force = false) {
    if (!this.progress) this.progress = {}
    if (!finish && current) {
      this.progress.finish = current
    } else {
      this.progress.current = current
      this.progress.finish = finish
    }
    if (!this.progress.current) this.progress.current = 0

    this.progress.percent = Math.floor(100 / this.progress.finish * (this.progress.current || 0))

    if (this._collectionName) {
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
