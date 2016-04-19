import { Job } from 'meteor/teamgrid:job-queue'

export class TestJob extends Job {
  process() {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve()
      }, Math.floor(Math.random() * (10000 - 3000 + 1)) + 3000)
    })
  }
}
