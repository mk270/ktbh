import waterfall

class KTBH(object):
    def __init__(self, config):
        self.router = waterfall.PipeRouter(config.get("main", "amqp_host"))

        self.queues = {}
        for q_name, conf_section, conf_item in [
            ("out", "main", "lp_queue"),
            ("broken", "main", "broken_lp_queue"),
            ("url", "main", "url_queue"),
            ("schema", "main", "schema_queue"),
            ("download", "main", "download_queue"),
            ("import", "main", "import_queue"),
            ("try_import", "main", "try_import_queue"),
            ("ready", "main", "ready_queue")
            ]:
            self.queues[q_name] = config.get(conf_section, conf_item)
        self.queues["error"] = "ktbh_errors"

    def get_queue(self, queue_name):
        return self.queues[queue_name]

    def list_queues(self):
        return self.queues.values()

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
                          error_queue=self.get_queue("error"))
