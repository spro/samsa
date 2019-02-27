Samsa = require '../src'

hello_client = new Samsa

main = ->
    reply = await hello_client.request 'hello', 'sayHello', {name: "Jeffrey"}
    console.log '[reply]', reply

main()

