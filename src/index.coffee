Kafka = require 'kafka-node'
Promise = require 'bluebird'
uuid = require 'uuid'
debug = require('debug')('samsa')

{EventEmitter} = require 'events'

exit = new EventEmitter
exiters = []

exitWrapped = ->
    await Promise.all exiters.map (e) -> e()
    process.exit()

exit.on 'exit', exitWrapped
# process.on 'exit', -> exit.emit 'exit'
process.on 'SIGINT', -> exit.emit 'exit'
process.on 'SIGTERM', -> exit.emit 'exit'

addPromiseMethods = (obj) ->
    Promise.promisifyAll obj, {suffix: 'Promise'}

module.exports = class Samsa extends EventEmitter

    constructor: (@name, @methods={}, @config={}) ->
        super()

        @host = @config.host or '127.0.0.1:9092'
        @kafka = addPromiseMethods new Kafka.KafkaClient({kafkaHost: @host})
        @createProducer()

        if @name?
            @createConsumerGroup()
        else
            @createConsumer()

        @consuming_topics = []
        @hooks = {}
        @subscriptions = {}
        @exitOnClose()

        @producer.on 'ready', (ready) =>
            debug('producer ready', ready)
            @emit 'producer ready'
        @producer.on 'error', (error) ->
            debug('producer error', error)

        debug('started')

    exitOnClose: ->
        exiters.push => new Promise (resolve, reject) =>
            @consumer.close ->
                resolve()

    close: ->
        @consumer.closePromise()

    createProducer: ->
        producer_options = {
            partitionerType: 1 # default = 0, random = 1, cyclic = 2, keyed = 3, custom = 4
        }
        @producer = addPromiseMethods new Kafka.Producer(@kafka, producer_options)

    createConsumerGroup: ->
        consumer_group_options = {
            kafkaHost: @host
            groupId: @name
        }
        @on 'producer ready', =>
            await @createTopics [@name, @name + '-response']
            @consumer = addPromiseMethods new Kafka.ConsumerGroup(consumer_group_options, [@name]) # Subscribes to topic of own name
            @consumer.on 'message', @handleMessage.bind(@)

    createConsumer: ->
        consumer_payloads = []
        consumer_options = {}
        @consumer = addPromiseMethods new Kafka.Consumer(@kafka, consumer_payloads, consumer_options)
        @consumer.on 'message', @handleMessage.bind(@)

    createTopics: (topics) ->
        created = await @producer.createTopicsPromise topics, true
        await @kafka.refreshMetadataPromise(topics)
        debug 'created topics', created

    consumeTopic: (topic) ->
        @consuming_topics.push topic
        added = await @consumer.addTopicsPromise [topic]
        debug 'added topic to consumer', added

    handleMessage: (full_message) ->
        debug('got message', full_message)
        message = JSON.parse full_message.value

        if hook = @hooks[message.id]
            delete @hooks[message.id]
            hook(message)

        else if method = @methods?[message.method]
            try
                response = await method(message.args...)
            catch err
                error = err
            id = message.id + '-response'
            sent = await @send message.service + '-response', {
                id
                response
                error
            }

        else if subscription = @subscriptions?[message.service]
            subscription(message)

    send: (topic, message) ->
        sent = await @producer.sendPromise [{
            topic: topic
            messages: [
                JSON.stringify message
            ]
        }]

    request: (service, method, args...) ->
        response_topic = service + '-response'
        if response_topic not in @consuming_topics
            await @consumeTopic response_topic
            debug('consuming', response_topic)
        id = uuid()
        hook = new Promise (resolve, reject) =>
            @hooks[id + '-response'] = (message) ->
                if message.error
                    reject message.error
                else
                    resolve message.response
        sent = await @send service, {
            id, service, method, args
        }
        debug('sent', sent)
        return hook

    subscribe: (service, subscription) ->
        if service not in @consuming_topics
            await @consumeTopic service
        @subscriptions[service] = subscription

