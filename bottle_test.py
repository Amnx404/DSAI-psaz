from bottle import route, run

@route('/api/3/percpu')
def hello():
    return "Hello World!"

run(host='localhost', port=61208, debug=True)