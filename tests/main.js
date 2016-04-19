// import './job-queue'

/* global describe it */
import { Mongo } from 'meteor/mongo'
import { assert } from 'meteor/practicalmeteor:chai'
import { JobQueue, Job } from 'meteor/teamgrid:job-queue'

class TestJob extends Job {
  process() {
    return new Promise((resolve, reject) => {
      const duration = Math.floor(Math.random() * (1000 - 300 + 1)) + 300
      setTimeout(() => {
        if (duration <= 650) {
          reject()
        } else {
          resolve()
        }
      }, duration)
    })
  }
}


describe('job queue', function jobQueueTests() {
  it('sets up the transform method on the collection', function transform(done) {
    this.timeout(300000)
    try {
      const collection = new Mongo.Collection('jobs')
      collection.remove({})
      assert.typeOf(collection._transform, 'null')

      const queue = new JobQueue({ collection })
      assert.typeOf(queue._options.collection._transform, 'function')

      queue.registerType('test', TestJob)
      for (let i = 1; i <= 100; i++) {
        queue.enqueueJob(new TestJob())
      }
      queue.startWorker({ concurrency: 2 })
    } catch (err) {
      done(err)
    }
  })
})
