class Node(object):
    
    def __init__(self, name, depends):
        self.name = name # e.g., 'addition1'
        self.depends = depends
        self.is_done = False
        
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
    
    def __init__(self, nodes, params):
        self.params = params
        self.nodes = nodes
        self.output = None
    
    def walk_dependencies(self, nodelist=None):
        if not nodelist:
            nodelist = self.nodes
        next_task = None
        while nodelist:
            n = nodelist.pop()
            if n.is_done:
                print "we ran {} already".format(n.name)
                return self.walk_dependencies([nodelist])
            
            elif n.can_run():
                print "we're ready to do {}".format(n.name)
                self.output = n.run(self.params)
                
            elif not n.can_run():
                print "{} has unmet dependencies".format(n.name)
                return self.walk_dependencies([n.depends])
                
            else:
                print 'what the fuck is happening??'
        
                
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
    
    print 'Running example scenario 1:'
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
    print 'The node list is {}'.format(node_list)
    d = DepGraph(node_list, new_params)
    d.walk_dependencies()
    
    # print output
    print 'The output is \n{}'format(d.output)
    
    print 'Running example scenario 2:'
    
    m2 = MultiplicationTask('m1', None)
    a2 = AdditionTask('a1', m1)
    node_list_rev = [a2, m2]
    
    print 'The node list is {}'.format(node_list_rev)
    
    d_rev = DepGraph(node_list_rev, new_params)
    d_rev.walk_dependencies()
    print 'The output is \n{}'.format(d_rev.output)
    