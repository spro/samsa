Samsa = require '../src'

hello_methods =
    sayHello: ({name}) -> "Hello, #{name}!"

hello_service = new Samsa 'hello', hello_methods
