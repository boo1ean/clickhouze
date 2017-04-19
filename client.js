const assert = require('assert')
const request = require('request')

module.exports = function createClient (config) {
	assert(config.host, 'host is required')
	assert(config.port, 'port is required')

	return {
		query: (queryText, cb) => {
			request({
				method: 'post',
				url: `http://${config.host}:${config.port}`,
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
