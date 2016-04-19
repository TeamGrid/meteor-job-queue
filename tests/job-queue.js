/* global describe it */
import { Mongo } from 'meteor/mongo'
import { assert } from 'meteor/practicalmeteor:chai'

import { JobQueue } from 'meteor/teamgrid:job-queue'

describe('job queue', () => {
  it('sets up the transform method on the collection', () => {
    const collection = new Mongo.Collection(null)
    assert.typeOf(collection._transform, 'undefined')

    const queue = new JobQueue({ collection })
    assert.typeOf(queue._options.collection._transform, 'function')
  })
})
