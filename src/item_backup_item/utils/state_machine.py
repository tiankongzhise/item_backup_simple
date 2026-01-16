
class StateMachine:
    '''A simple state machine class'''

    '''Initialize the state machine with a list of states and the current state set to None.'''
    _instance = None
    _is_initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(StateMachine, cls).__new__(cls)
        return cls._instance


    def __init__(self):
        if self._is_initialized:
            return
        self._is_initialized = True
        self.state_list = ['classify','hashed','zipped','zip_file_hashed','unzipped','unzip_hashed','uploaded','delete']
        self.state_dict = {state: i for i, state in enumerate(self.state_list)}
        print(f'state_dict:{self.state_dict}')
        self.current_state = None
    
    def set_state(self, state):
        self.current_state = state
    
    def get_current_state(self) -> str:
        '''Return the current state of the state machine.'''
        if self.current_state is None:
            raise ValueError("Current state is None, please use .set_state to set current state first")
        return self.current_state

    def get_next_state(self):
        if self.current_state is None:
            raise ValueError("Current state is None, please use .set_state to set current state first")
        current_index = self.state_dict[self.current_state]
        next_index = current_index + 1
        if next_index >= len(self.state_list):
            print("Next state is None, current state is the last state")
            return None
        return self.state_list[next_index]
    def get_previous_state(self):
        if self.current_state is None:
            raise ValueError("Current state is None, please use .set_state to set current state first")
        current_index = self.state_dict[self.current_state]
        previous_index = current_index - 1
        if previous_index < 0:
            print("Previous state is None, current state is the first state")
            return None
        return self.state_list[previous_index]
    def get_state_by_index(self, step:int):
        '''
        从1开始计数，1表示第一个状态,-1表示最后一个状态
        '''
        if step == -1:
            return self.state_list[-1]
        index = step - 1
        if index < 0 or index >= len(self.state_list):
            print("State is None, index is out of range")
            return None
        return self.state_list[index]

def get_state_machine():
    """Get the singleton instance of the StateMachine class."""
    machine = StateMachine()
    print("Get state machine instance")
    try:
        print(f"Current state: {machine.get_current_state()}")
    except Exception as e:
        print(f"Error: {e}")
    return machine
