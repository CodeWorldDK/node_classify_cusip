def node(name, inputs, output, params):
    def wrapper(func):
        func._node_meta = {
            "name": name,
            "inputs": inputs,
            "output": output,
            "params": params
        }
        return func
    return wrapper
