import logging

class Node(object):

    def __init__(self, name, depends):
        self.name = name # e.g., 'addition1'
        self.depends = depends
        self.is_done = False
        self.error = False

    def update_state(self):
        self.is_done = True

    def check_dependencies(self):
        if not self.depends:
            depends_met = [True]
        elif isinstance(self.depends, list):
            depends_met = [i.is_done for i in self.depends]
        else:
            depends_met = [self.depends.is_done]
        return depends_met

    def can_run(self):
        depends_met = self.check_dependencies()
        if all(depends_met):
            return True
        else:
            return False

class DepGraph(object):

    def __init__(self, nodes, params, verbose):
        self.params = params
        self.nodes = nodes
        self.completed = []
        self.next_task = None
        self.output = None
        self.verbose = verbose

    def run_task(self):
        '''execute the run() method for a node and update progress variables'''
        self.output = self.next_task.run(self.params)
        if self.next_task.error:
            return False
        else:
            self.params = self.output
            self.completed.append(self.next_task)
            self.next_task = None
            return True

    def walk_dependencies(self):
        '''walk list of nodes tasks and run them when their dependencies are met'''

        logging.info('Processing file "{}".'.format(self.params['file']))
        while self.nodes:

            # previous runs that produced outputs should stop execution of the workflow
            # even if nodes remain in the nodelist
            if self.params['workflow_done']:
                logging.info('Workflow completed')
                return

            # identify a focal task to evaluate
            if not self.next_task:
                self.next_task = self.nodes.pop()

            # if the task has been done, add it to the completed list and reset next task
            if self.next_task.is_done:
                logging.info('...We already completed task "{}"'.format(self.next_task.name))
                self.completed.append(self.next_task)
                self.next_task = None

            # if the task is ready to run then do so and check for errors in output
            elif self.next_task.can_run():
                logging.info('...We are ready to run task "{}"'.format(self.next_task.name))
                runner = self.run_task()
                # if the next task returns an error, return the function for clean exit
                if not runner:
                    return

            # if there are dependencies, add the focal task back to the queue and
            # set self.next_task to the dependency
            elif not self.next_task.can_run():
                logging.info(
                    '...Task "{}" has unmet dependency "{}"'.format(
                        self.next_task.name, self.next_task.depends
                    )
                )
                tmp_depends = self.next_task.depends
                self.nodes.append(self.next_task)
                self.next_task = tmp_depends 

            else:
                logging.critical('...Something went wrong... unable to walk dependencies.')


class MultiplicationTask(Node):

    def __init__(self, name, depends):
        Node.__init__(self, name, depends)
        self.outputs = None

    # do work
    def run(self, params):
        x = params['x']
        m = params['m']
        mx = [i*m for i in x]
        params['mx'] = mx
        self.outputs = params
        self.update_state()
        return params


class AdditionTask(Node):

    def __init__(self, name, depends):
        Node.__init__(self, name, depends)
        self.output = None

    # do work
    def run(self, params):
        mx = params['mx']
        b = params['b']
        y = [i+b for i in mx]
        params['y'] = y
        self.output = params
        self.update_state()
        return params


if __name__ == '__main__':

    print('Running example scenario 1:')
    new_params = {
        'b': 4.7,
        'm': 2.1,
        'x': range(10)
    }

    # instantiate task classes
    m1 = MultiplicationTask('m1', None)
    a1 = AdditionTask('a1', m1)

    # resolve dependencies and execute tasks
    node_list = [m1, a1]
    print('The node list is {}'.format(node_list))
    d = DepGraph(node_list, new_params)
    d.walk_dependencies()

    # print output
    print('The output is \n{}'.format(d.output))

    print('Running example scenario 2:')

    m2 = MultiplicationTask('m1', None)
    a2 = AdditionTask('a1', m1)
    node_list_rev = [a2, m2]

    print('The node list is {}'.format(node_list_rev))

    d_rev = DepGraph(node_list_rev, new_params)
    d_rev.walk_dependencies()
    print('The output is \n{}'.format(d_rev.output))
