const assert = require('assert')
const request = require('request')

module.exports = function createClient (config) {
	assert(config.host, 'host is required')
	assert(config.port, 'port is required')

	let clickhouseServerUrl = `http://${config.host}:${config.port}`
	if (config.database) {
		clickhouseServerUrl += '?database=' + config.database
	}
	let clickhouseParams = {
		method: 'post',
		url: clickhouseServerUrl	
	}
	if (config.user && config.password) {
		clickhouseParams.auth = { 
			user: config.user,
			pass: config.password
		}
	}
	
	return {
		query: (queryText, cb) => {
			clickhouseParams.body = queryText
			request(clickhouseParams, (err, res) => {
				if (cb) {
					cb(err, res)
				} else {
					throw err
				}
			})
		}
	}
}
