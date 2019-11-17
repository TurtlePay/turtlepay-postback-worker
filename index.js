// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

require('dotenv').config()
const cluster = require('cluster')
require('colors')
const Config = require('./config.json')
const cpuCount = Math.ceil(require('os').cpus().length / 8)
const Logger = require('./lib/logger')
const MessageSigner = require('./lib/messageSigner')
const RabbitMQ = require('./lib/rabbit')
const request = require('request-promise-native')
const signer = new MessageSigner()
const util = require('util')

function spawnNewWorker () {
  cluster.fork()
}

if (cluster.isMaster) {
  if (!process.env.NODE_ENV || process.env.NODE_ENV.toLowerCase() !== 'production') {
    Logger.warning('Node.js is not running in production mode. Consider running in production mode: export NODE_ENV=production'.yellow)
  }

  Logger.log('Starting TurtlePay Postback Service...')

  for (var cpuThread = 0; cpuThread < cpuCount; cpuThread++) {
    spawnNewWorker()
  }

  cluster.on('exit', (worker, code, signal) => {
    Logger.error('Worker %s died', worker.process.pid)
    spawnNewWorker()
  })
} else if (cluster.isWorker) {
  const rabbit = new RabbitMQ(process.env.RABBIT_PUBLIC_SERVER || 'localhost', process.env.RABBIT_PUBLIC_USERNAME || '', process.env.RABBIT_PUBLIC_PASSWORD || '', false)

  rabbit.on('log', log => {
    Logger.log('[RABBIT] %s', log)
  })

  rabbit.on('connect', () => {
    Logger.log('[RABBIT] connected to server at %s', process.env.RABBIT_PUBLIC_SERVER || 'localhost')
  })

  rabbit.on('disconnect', (error) => {
    Logger.error('[RABBIT] lost connected to server: %s', error.toString())
    cluster.worker.kill()
  })

  rabbit.on('message', (queue, message, payload) => {
    if (!payload.request.callback) {
      Logger.warning('Worker #%s: Caller did not provide a callback for %s', cluster.worker.id, payload.address)
      return rabbit.ack(message)
    }

    if (payload.request.callback.substring(0, 4).toLowerCase() !== 'http') {
      /* They didn't supply a valid callback, we're done here */
      Logger.warning('Worker #%s: No valid callback location available for processed payment to %s [%s]', cluster.worker.id, payload.address, payload.status)
      return rabbit.ack(message)
    }

    /* Build what we're going to try to send back */
    const postbackPayload = {
      address: payload.address,
      status: payload.status,
      request: {
        address: payload.request.address,
        amount: payload.request.amount,
        userDefined: payload.request.callerData
      }
    }

    /* If we have a transaction hash add that in */
    if (payload.transactionHash) {
      postbackPayload.txnHash = payload.transactionHash

      /* If we also have a transaction private key, let's send that back as well */
      if (payload.transactionPrivateKey) {
        postbackPayload.txnPrivateKey = payload.transactionPrivateKey
      }
    }

    /* If we have an amountReceived, add that in */
    if (payload.amountReceived) {
      postbackPayload.amountReceived = payload.amountReceived
    }

    /* If we have an amountSent, add that in */
    if (payload.amountSent) {
      postbackPayload.amountSent = payload.amountSent
      postbackPayload.amount = payload.amountSent // This is provided for legacy support
    }

    /* If we have a networkFee, add that in */
    if (payload.networkFee) {
      postbackPayload.networkFee = payload.networkFee
    }

    /* If we have an amount, add that in */
    if (payload.amount) {
      postbackPayload.amount = payload.amount
    }

    /* If we have blocksRemaining add that in */
    if (payload.blocksRemaining) {
      postbackPayload.blocksRemaining = payload.blocksRemaining
    }

    /* If we have confirmationsRemaining add that in */
    if (payload.confirmationsRemaining) {
      postbackPayload.confirmationsRemaining = payload.confirmationsRemaining
    }

    /* If we have the keys for the one-time use wallet, add that in */
    if (payload.keys) {
      postbackPayload.keys = payload.keys
    }

    /* If we have a URL that we can post to, then we're going to give it a try */
    return request({
      url: payload.request.callback,
      method: 'POST',
      json: true,
      body: postbackPayload,
      headers: {
        digest: util.format('SHA-256=', signer.digest(postbackPayload, 'base64'))
      },
      httpSignature: {
        algorithm: 'rsa-sha256',
        headers: [
          '(request-target)',
          'date',
          'digest'
        ],
        keyId: postbackPayload.address,
        key: Buffer.from(payload.privateKey, 'hex')
      },
      timeout: Config.postTimeout
    })
      .then(() => {
      /* Success, we posted the message to the caller */
        Logger.info('Worker #%s: Successfully delivered [%s] message for %s to %s', cluster.worker.id, payload.status, payload.address, payload.request.callback)
        return rabbit.ack(message)
      })
      .catch(() => {
      /* Success, we posted the message to the caller */
        Logger.error('Worker #%s: Failed to deliver [%s] message for %s  to %s', cluster.worker.id, payload.status, payload.address, payload.request.callback)
        return rabbit.ack(message)
      })
  })

  rabbit.connect()
    .then(() => { return rabbit.createQueue(Config.queues.complete, true) })
    .then(() => { return rabbit.registerConsumer(Config.queues.complete, 1) })
    .then(() => { Logger.log('Worker #%s awaiting requests', cluster.worker.id) })
    .catch(error => {
      Logger.error('Error in worker #%s: %s', cluster.worker.id, error.toString())
      cluster.worker.kill()
    })
}
