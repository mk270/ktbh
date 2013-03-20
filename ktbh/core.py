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
            ("download", "main", "download_queue")
            ]:
            self.queues[q_name] = config.get(conf_section, conf_item)
        self.queues["error"] = "ktbh_errors"

    def delete_all_queues(self):
        for q in self.queues.values():
            self.router.delete_queue(q)

    def add_landing_page(self, url):
        payload = {
            "url": url
            }
        self.router.hand_off_json(self.queues["out"], payload)

    def wrap_callback(self, callback):
        def _callback(body):
            return [ (self.queues[q], msg) for q, msg in callback(body) ]
        return _callback

    def run_pipe(self, input_queue, callback):
        self.router.route(callback=self.wrap_callback(callback),
                          input_queue=self.queues[input_queue],
                          error_queue=self.queues["error"])
