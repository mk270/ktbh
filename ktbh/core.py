import waterfall

class KTBH(object):
    def __init__(self, config):
        self.router = waterfall.PipeRouter(config.get("main", "amqp_host"))

        self.queue_prefix = config.get("main", "queue_prefix")

    def get_queue(self, queue_name):
        return self.queue_prefix + queue_name

    def list_queues(self):
        assert False

    def delete_all_queues(self):
        for q in self.list_queues():
            self.router.delete_queue(q)

    def add_landing_page(self, url):
        payload = {
            "url": url
            }
        self.router.hand_off_json(self.get_queue("out"), payload)

    def wrap_callback(self, callback):
        def _callback(body):
            return [ (self.get_queue(q), msg) for q, msg in callback(body) ]
        return _callback

    def run_pipe(self, input_queue, callback):
        self.router.route(callback=self.wrap_callback(callback),
                          input_queue=self.get_queue(input_queue),
                          error_queue=self.get_queue("errors"))
