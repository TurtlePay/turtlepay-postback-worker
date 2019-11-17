// Copyright (c) 2019, The TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

require('colors')
const util = require('util')

class Logger {
  static debug () {
    const message = createMessage(arguments)
    return log(util.format('[DEBUG] %s', message).blue)
  }

  static error () {
    const message = createMessage(arguments)
    return log(util.format('[ERROR] %s', message).red)
  }

  static info () {
    const message = createMessage(arguments)
    return log(util.format('[INFO] %s', message).green)
  }

  static log () {
    const message = createMessage(arguments)
    return log(message)
  }

  static warning () {
    const message = createMessage(arguments)
    return log(util.format('[WARNING] %s', message).yellow)
  }
}

function createMessage (entry) {
  const args = []

  for (var i = 0; i < entry.length; i++) {
    args.push(entry[i])
  }

  return util.format(args.shift(), ...args)
}

function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

module.exports = Logger
