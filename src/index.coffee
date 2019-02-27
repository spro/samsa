Kafka = require 'kafka-node'
Promise = require 'bluebird'
uuid = require 'uuid'

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

module.exports = class Samsa

    constructor: (@name, @methods={}, @config={}) ->
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

    exitOnClose: ->
        exiters.push => new Promise (resolve, reject) =>
            @consumer.close ->
                resolve()

    close: ->
        @consumer.closePromise()

    createProducer: ->
        producer_options = {
            partitionerType: 1
        }
        @producer = addPromiseMethods new Kafka.Producer(@kafka, producer_options)

    createConsumerGroup: ->
        consumer_group_options = {
            kafkaHost: @host
            groupId: @name
        }
        @consumer = addPromiseMethods new Kafka.ConsumerGroup(consumer_group_options, [@name])
        @consumer.on 'message', @handleMessage.bind(@)

    createConsumer: ->
        if @name?
            consumer_payloads = [{topic: @name}]
            consumer_options = {groupId: @name}
        else
            consumer_payloads = []
        @consumer = addPromiseMethods new Kafka.Consumer(@kafka, consumer_payloads, consumer_options)
        @consumer.on 'message', @handleMessage.bind(@)

    createTopic: (topic) ->
        @kafka.createTopicsPromise [topic]

    consumeTopic: (topic) ->
        @consuming_topics.push topic
        await @createTopic topic
        added = await @consumer.addTopicsPromise [topic]

    handleMessage: (full_message) ->
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
        if service not in @consuming_topics
            await @consumeTopic service + '-response'
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
        return hook

    subscribe: (service, subscription) ->
        if service not in @consuming_topics
            await @consumeTopic service
        @subscriptions[service] = subscription

