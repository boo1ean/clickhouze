const assert = require('assert')
const request = require('request')

module.exports = function createClient (config) {
	assert(config.host, 'host is required')
	assert(config.port, 'port is required')

	let clickhouseServerUrl = `http://${config.host}:${config.port}`
	if (config.database) {
		clickhouseServerUrl += '?database=' + config.database
	}

	return {
		query: (queryText, cb) => {
			request({
				method: 'post',
				url: clickhouseServerUrl,
				body: queryText
			}, (err, res) => {
				if (cb) {
					cb(err, res)
				} else {
					throw err
				}
			})
		}
	}
}
