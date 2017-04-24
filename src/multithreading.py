from __future__ import print_function
import sys
import os
import threading
import time
import Pyro4
from constants import config
import datetime as dt


# Use dill to serialize Pyro objects
# Pyro4.config.SERIALIZER = 'dill'
# Pyro4.config.SERIALIZERS_ACCEPTED = {'dill', 'json', 'pickle', 'marshal', 'serpent'}

# For easier trace when things crash in remote objects
sys.excepthook = Pyro4.util.excepthook


@Pyro4.expose # to expose this class to Pyro
class Gateway_frontend(object):
    """
    Class representing the frontend of the gateway
    """
    
    def __init__(self, name, daemon):
        """
        Constructor        
        
        Intializes the gateway and registers it with the daemon object
        
        Args:
        name: The name of the frontend
        daemon: Pyro4 daemon object
        """

        self.sensors        = {}           # dictionary, key = global id of object, value = (name, URI of object)
        self.devices        = {}           # dictionary, key = global id of object, value = (name, URI of object)
        self.backend        = None         # place for a dictionary {'id':obj_id, 'uri':uri}
        self.security       = None         # place for a dictionary {'id':obj_id, 'uri':uri}
        self.uri            = daemon.register(self) # registering Pyro daemon
        self.name           = name

        print(self.name)
        print(self.uri)

        #Determine the name of the replica
        if self.name == "Gateway 1 Frontend":
            self.dev_id = config.GATEWAY_1_FRONTEND
            self.other_gf_name = "Gateway 2 Frontend"
        elif self.name == "Gateway 2 Frontend":
            self.dev_id = config.GATEWAY_2_FRONTEND
            self.other_gf_name = "Gateway 1 Frontend"

        # Register with the nameserver
        with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
            ns.register(self.name, self.uri)
            print("Registered", self.name, "with nameserver")

        self.states_cache = {}         # Cache 
        self.replica_sensors=dict() # a dictionary storing with the replica name as key and its sensors as values
        self.replica_devices=dict()# a dictionary storing with the replica name as key and its devices as values

        # Start the daemon thread to check if the replica is alive
        t=threading.Thread(target=self.ping_replicas) 
        t.daemon=True
        t.start()

        # A list to maintain LRU in cache
        self.cache_recency_list = []

        # Boolean flag to simulate a crash
        self.simulate_crash = False

        self.cache_lock=threading.Lock()

        if len(sys.argv)>1 and sys.argv[1]=='no_cache':
            self.use_cache=False
            with open('latency.dat','w') as f:
                pass
        else:
            self.use_cache=True
            with open('cache_latency.dat','w') as f:
                pass
            
        # Start the timer for crash if the number of arguments is more than 1
        # if len(sys.argv)>1:
        #     t=threading.Thread(target=self.crash)
        #     t.daemon=True
        #     t.start()
        return


    def get_id(self):
        """
        Function to get the device id
        
        Returns: the device id of the process
        """
        return self.dev_id
    

    def register(self, obj_type, name, uri):
        """
        Function to register objects
        
        This function registers all the sensors, devices and the backend with the gateway. It provides every entity with a unique id as well.

        Args:
        obj_type: Denotes the type of the object being registered. Can be sensor, device or backend
        name: The name of the object
        uri: The uri of the object

        Returns:
        obj_id: The id of the object
        """

        if obj_type.lower() == 'sensor' or obj_type.lower() == 'device' or \
           obj_type.lower() == 'backend' or obj_type.lower() == 'security':

            with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                other_gf_uri = ns.lookup(self.other_gf_name)
                with Pyro4.Proxy(other_gf_uri) as other_gf:
                    other_gf_sensors = other_gf.get_sensors()
                    other_gf_devices = other_gf.get_devices()

                num_other_gateway_objs = len(other_gf_sensors) + len(other_gf_devices)

            # Generate the object id
            obj_id = num_other_gateway_objs + len(self.sensors) + len(self.devices)

            if obj_type.lower() == 'sensor':
                # If the client is a sensor
                self.sensors[obj_id] = (name, uri)

                # Inform the replica
                with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                    other_gf_uri = ns.lookup(self.other_gf_name)
                    with Pyro4.Proxy(other_gf_uri) as other_gf:
                        other_gf.register_from_replica(self.name, obj_id, obj_type, name, uri)
            elif obj_type.lower() == 'device':
                # if the client is a device
                self.devices[obj_id] = (name, uri)

                # Inform the replica
                with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                    other_gf_uri = ns.lookup(self.other_gf_name)
                    with Pyro4.Proxy(other_gf_uri) as other_gf:
                        other_gf.register_from_replica(self.name, obj_id, obj_type, name, uri)

            elif obj_type.lower() == 'backend':
                # If the backend is connecting

                # Determine which frontend is this backend associated with
                if self.name == "Gateway 1 Frontend":
                    obj_id = config.GATEWAY_1_BACKEND
                elif self.name == "Gateway 2 Frontend":
                    obj_id = config.GATEWAY_2_BACKEND
                self.backend = {'id':obj_id, 'uri':uri}

                
            elif obj_type.lower() == 'security':
                # If the security system is the client
                obj_id = config.SECURITY_SYSTEM
                self.security = {'id':obj_id, 'uri':uri}

                # Inform the replica 
                with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                    other_gf_uri = ns.lookup(self.other_gf_name)
                    with Pyro4.Proxy(other_gf_uri) as other_gf:
                        other_gf.register_from_replica(self.name, obj_id, obj_type, name, uri)

            if obj_type.lower() != 'backend' and obj_type.lower() != 'security': # If object is not the backend or security, register the object with the backend as well
                t = threading.Thread(target=self.rmi_call, args=(self.backend['uri'], 'register', (obj_id, name)))
                t.daemon = False
                t.start()

            if name == 'temperature':
                self.run_query_temp_loop()

            print("\nRegistered " + name + " with id = " + str(obj_id))

            return obj_id

        else:
            print('Invalid object type. Object not registered.')
            return -1


    def query_state(self, to_obj_id):
        """
        Function to query the state of the object

        This method takes the id of the object to query and sends a pull request to the object to get its state
        
        Args:
        to_obj_id: The id of the object to query

        """

        # Get the uri of the object
        if self.sensors.has_key(to_obj_id):
            _, uri = self.sensors[to_obj_id]
        elif self.devices.has_key(to_obj_id):
            _, uri = self.devices[to_obj_id]
        else:
            print('No object exists with ID', to_obj_id)
            return

        # Call the query state on the object
        t = threading.Thread(target=self.rmi_call, args=(uri, 'query_state', to_obj_id))
        t.daemon = False
        t.start()

        if len(sys.argv)>1 and sys.argv[1]=='1' and self.simulate_crash:
            time.sleep(0.5)
            print("++++++++++++++++ CRASH +++++++++++++++++")
            with Pyro4.Proxy(self.backend['uri']) as back:
                try:
                    back.force_close()
                except:
                    pass
                
                os._exit(2)
            print ("Done")
        return


    def report_state(self, from_obj_id, state, name, timestamp, forwarded=False):
        """
        Function called by devices and sensors to report their state
        
        This method is called whenever a device or sensor wants to push its state (or respond to a pull query)

        Args:
        from_obj_id: The caller's id
        state: The state reported
        timestamp: Time stamp of the send event
        """

        # Get the uri and name of the object
        if self.sensors.has_key(from_obj_id):
            name, uri = self.sensors[from_obj_id]
            print("\nSensor for", name, "reported state", state)
        elif self.devices.has_key(from_obj_id):
            name, uri = self.devices[from_obj_id]
            print("\nSmart", name, "reported state", state)

        if forwarded:
            print("\n[Forwarded from replica] Object ID", from_obj_id, "reported state", state)

        # Check if the state being reported is valid
        # if name == 'temperature' and is_real(state):
        #     pass
        # elif state == 0 or state == 1:
        #     pass
        # else:
        #     print('Invalid state.')
        #     return

        # Update cache
        if self.use_cache:
            self.update_cache(from_obj_id, state)

        # Report back to backend
        t = threading.Thread(target=self.rmi_call, args=(self.backend['uri'], 'report_state', (from_obj_id, state, name, timestamp)))
        t.daemon = False
        t.start()
        
        
        # Forward this state update to other frontend also
        if not forwarded:
            with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                other_gf_uri = ns.lookup(self.other_gf_name)
                # Inform the replica if its alive
                try:
                    with Pyro4.Proxy(other_gf_uri) as replica:
                        replica.is_alive()

                    t = threading.Thread(target=self.rmi_call, args=(other_gf_uri, 'report_state', (from_obj_id, state, name, timestamp, True)))
                    t.daemon = False
                    t.start()
                except:
                    pass

        if len(sys.argv)>1 and sys.argv[1]=='2' and self.simulate_crash:
            time.sleep(0.5)
            print("++++++++++++++++ CRASH +++++++++++++++++")
            with Pyro4.Proxy(self.backend['uri']) as back:
                try:
                    back.force_close()
                except:
                    pass

                os._exit(2)
            print ("Done")
        return

    def change_state(self, to_obj_id, state):
        """
        Function to change the state of a device
        
        This method changes the state of the target device

        Args:
        to_obj_id: The target device's id
        state: the state to change to
        """

        # Get the device URI
        if self.devices.has_key(to_obj_id):
            _, uri = self.devices[to_obj_id]
        else:
            print('No device exists with ID', to_obj_id)
            return

        # Check the validity of the target state
        if state != 0 and state != 1:
            print('Invalid state.')
            return

        # Call the change state function on the device
        t = threading.Thread(target=self.rmi_call, args=(uri, 'change_state', (to_obj_id, state)))
        t.daemon = False
        t.start()


        return

    def run_query_temp_loop(self):
        """
        Starts and runs a new thread to perform perdioc query for temperature 
        """
        t = threading.Thread(target=self.query_temp_loop)
        t.daemon = True
        t.start()
        return

    def query_temp_loop(self):
        """
        Periodically queries the temperature sensor for its state 
        """
        while True:
            time.sleep(3) # wait for some time before running again
            for key in self.sensors.keys():
                name, _ = self.sensors[key]
                if name.lower() == 'temperature':
                    self.query_state(key)
                    continue


    def rmi_call(self, uri, method_name, arg_tuple):
        """
        Method to facilitate RMI calls in threads.
        This method will be provided as the target for new threads
        with appropriate arguments in order call specific methods
        on a remote object.

        Args:
        - uri: URI of the object on which to call the method
        - method_name: the exact name of the method to call
        - arg_tuple: the arguments to pass to the method
        """

        # Actual remote method invocation
        with Pyro4.Proxy(uri) as obj:
            if method_name == 'register':
                if arg_tuple is None:
                    obj.register()
                else:
                    obj.register(*arg_tuple)
            elif method_name == 'query_state':
                obj.query_state(arg_tuple)
            elif method_name == 'report_state':
                obj.report_state(*arg_tuple)
            elif method_name == 'change_state':
                obj.change_state(*arg_tuple)

        return

    def get_sensors(self):
        """
        Function to get all the registered sensors
        
        Returns: Dictionary of all sensors registered (key: object ID, value: (name, uri))
        """
        return self.sensors

    def get_devices(self):
        """
        Function to get all the registered devices
        
        Returns: Dictionary of all devices registered (key: object ID, value: (name, uri))
        """
        return self.devices

    def has_backend(self):
        '''
        Function to check if the backend has registered or not
        
        ---
        Returns:
        True, if the backend is registered, else False
        '''
        return self.backend is not None

    def ping_replicas(self):
        '''
        Function to ping the replicas to see if they are alive
        
        This function runs as a periodic loop and checks whether the replica is alive or not. 
        If the replica has failed, it connects with the clients of those replicas and associates them with itself.
        

        '''        
        while True:
            time.sleep(2)
            with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
                uri=ns.lookup(self.other_gf_name)

                with Pyro4.Proxy(uri) as replica:
                    #See if the replica is alive
                    try:
                        replica.is_alive()
                        print("\nReplica is alive. No action needed.")
                    except:
                        # Take over the load
                        print("\nReplica ("+self.other_gf_name+") FAILED! Taking control of its clients...")
                        self.distribute_failed_load(self.other_gf_name)
                        break
        return

    def is_alive(self):
        '''
        Function to check if the gateway is alive or not
        '''
        return


    def register_from_replica(self,replica_name, cl_id, cl_type, cl_name, client_uri):
        '''
        Function called by the replica to inform of a registeration at its end
        
        ---
        Args:
        replica_name: The name of the replica
        cl_id: The ID of the client 
        cl_type: The type (sensor, device, security) of the client registered
        client_uri: The uri of the client registered
        '''
        
        if cl_type=='sensor':
            if replica_name in self.replica_sensors: # See if the replica had registered some client previously
                # Append to the existing list
                self.replica_sensors[replica_name].append(client_uri)
            else:
                #Create a new list and append the client
                self.replica_sensors[replica_name]=list()
                self.replica_sensors[replica_name].append(client_uri)

            print("\nReceived registration info for object ID", cl_id, cl_name, "sensor from", replica_name)

        elif cl_type=='device': 
            if replica_name in self.replica_devices: # See if the replica had registered some device previously
                # Append to the existing list
                self.replica_devices[replica_name].append(client_uri)
            else:
                #Create a new list and append the client
                self.replica_devices[replica_name]=list()
                self.replica_devices[replica_name].append(client_uri)

            print("\nReceived registration info for ", cl_id, "smart", cl_name, "from", replica_name)

        else: # security system
            self.security = {'id':cl_id, 'uri':client_uri}
            print("\nReceived registration info for security system from", replica_name)

        return

    def distribute_failed_load(self,failed_replica_name):
        '''
        Function called to deal with the failure of the replica

        This function contacts the clients associated with the replica and associates itself with them
        so that any future communication occurs with the caller frontend
        
        ---
        Args:
        failed_replica_name: The name of the replica which failed

        '''
        print("Distributing failed load")
        
        # If the failed replica had some sensors associated with it

        if failed_replica_name in self.replica_sensors:

            # Get the list of sensors associated with the replica
            sensor_list=self.replica_sensors[failed_replica_name]

            #Contact every sensor and associate itself as the frontend to contact
            for sensor_uri in sensor_list:
                with Pyro4.Proxy(sensor_uri) as sensor:
                    s_id=sensor.get_id()
                    self.sensors[s_id]=(sensor.get_name(),sensor_uri)
                    sensor.set_gateway(self.name)
                    print("Adopted", sensor.get_name(), "from", failed_replica_name)

                    #Inform the backend of this association
                    t = threading.Thread(target=self.rmi_call, args=(self.backend['uri'], 'register', (s_id, sensor.get_name())))
                    t.daemon = False
                    t.start()

                    
                    if sensor.get_name() == 'temperature':
                        #Start the temperature query loop 
                        self.run_query_temp_loop()

                        
        if failed_replica_name in self.replica_devices:         # If the failed replica had some sensors associated with it
            # Get the device list
            device_list=self.replica_devices[failed_replica_name]

            # Contact every device and associate itself as the frontend to contact
            for device_uri in device_list:
                with Pyro4.Proxy(device_uri) as device:
                    d_id=device.get_id()
                    self.devices[d_id]=(device.get_name(),device_uri)
                    device.set_gateway(self.name)
                    print("Adopted", device.get_name(), "from", failed_replica_name)

                    # Inform the backend of this association
                    t = threading.Thread(target=self.rmi_call, args=(self.backend['uri'], 'register', (d_id, device.get_name())))
                    t.daemon = False
                    t.start()

        # Associate itself with the security system
        if self.security is not None:
            with Pyro4.Proxy(self.security['uri']) as sec:
                sec.set_gateway(self.name)
                print("Adopted security system from", failed_replica_name)

        return

    def get_states_history(self):
        '''
        Function to get the history of the states from backend
        
        ---
        Returns:
        history of states from backend

        '''
        print("\nSecurity system requested for states history... Fetching and returning...")
        with Pyro4.Proxy(self.backend['uri']) as gb:
            history = gb.get_states_history()
        return history

    def update_cache(self, from_obj_id, state=None):

        '''
        Function to update the cache and the LRU list
        
        If state is None, this function just puts the client as the most recently used object
        If its a write request, it either modifies the cache if the object already has an entry or removes the least recently
        used object's entry and adds this objects entry
        
        ---
        Args:
        from_obj_id: The id of the client from which the request arrived
        state: The state of the client

        '''

        if state is not None:
            print('\nWriting in cache for object ID', from_obj_id)
        else:
            print('\nReading from cache for object ID', from_obj_id)

        # if cache size limit reached, remove the first item from recency list (least recently used) and its cache item
        self.cache_lock.acquire()
        if from_obj_id in self.cache_recency_list:
            print('Object ID already in cache. Updating item recency list.')
            # if from_obj_id is already in recency list
            self.cache_recency_list.remove(from_obj_id)
        if len(self.cache_recency_list) == config.MAX_CACHE_ITEMS and state is not None:
                print('Cache item limit reached. Removing least recently used item.')
                # if from_obj_id is not in recency list, but need to add it to cache
                del self.states_cache[self.cache_recency_list[0]]
                del self.cache_recency_list[0]

        # append the incoming from_obj_id to recency list
        self.cache_recency_list.append(from_obj_id)

        if state is not None:
            print('Updated cache item with state', state)
            self.states_cache[from_obj_id] = state

        print('Cache recency list:', self.cache_recency_list)
        print('Cache dictionary:', self.states_cache)
        self.cache_lock.release()

        return

    def get_state_by_obj_id(self, obj_id):
        '''
        Function to get the state of an object

        This function first checks the cache of the frontend for the object's state and in case of a miss, 
        it retrieves from the backend and updates the cache
        
        ---
        Args:
        obj_id: The id of the object whose state is to be retrieved

        ---
        Returns:
        state of the object
        '''

        # Simulate a crash if its asked
        if len(sys.argv)>1 and sys.argv[1]=='3' and self.simulate_crash:
            time.sleep(0.5)
            print("++++++++++++++++ CRASH +++++++++++++++++")
            with Pyro4.Proxy(self.backend['uri']) as back:
                try:
                    back.force_close()
                except:
                    pass
            os._exit(2)
                
        # try getting it from cache if possible
        if self.use_cache and self.states_cache.has_key(obj_id):
            t_begin=dt.datetime.now()
            self.update_cache(obj_id)
            print('\nResponding to get_state_by_obj_id request. Returning value from cache for object ID', obj_id)
            t_end=dt.datetime.now()
            resp_time=t_end-t_begin
            print("CACHE: hit => ",resp_time.total_seconds())
            with open('cache_latency.dat','a') as f:
                f.write('Hit ,'+str(resp_time.total_seconds())+'\n')
            return self.states_cache[obj_id]
        # else get it from backend
        else:
            t_begin=dt.datetime.now()
            with Pyro4.Proxy(self.backend['uri']) as gb:
                states = gb.get_latest_states()
                print ("staes = ",states)
            print ("staes = ",states)
            if states.has_key(obj_id):
                if self.use_cache:
                    self.update_cache(obj_id, states[obj_id])
                print('\nResponding to get_state_by_obj_id request. Returning value from database for object ID', obj_id)
                t_end=dt.datetime.now()
                resp_time=t_end-t_begin
                if self.use_cache:
                    print("CACHE: miss => ",resp_time.total_seconds())
                    with open('cache_latency.dat','a') as f:
                        f.write('Miss ,'+str(resp_time.total_seconds())+'\n')
                else:
                    with open('latency.dat','a') as f:
                        f.write(str(resp_time.total_seconds())+'\n')
                return states[obj_id]
            else:
                return -1
            
    def crash(self):
        if len(sys.argv)>1:
            # print("Set crash timer")
            # time.sleep(16)
            print ("++++++++++ Setting crash var +++++++++++++")
            self.simulate_crash=True
        return

def is_real(n):
    """
    Function to check if n is real or not
        
    Args:
    n: the number to check
    """
    try:
        float(n)
    except ValueError:
        return False
    return True


def main():
    """
    Runner for the gateway frontend
    """

    with Pyro4.locateNS(port=config.NAME_SERVER) as ns:
        pyro_obj_dict = ns.list()
        if 'Frontend' in ' '.join(pyro_obj_dict.keys()):
            # A frontend object already exists; start as second frontend
            gf_port = config.GATEWAY_2_FRONTEND
            gf_name = 'Gateway 2 Frontend'
        else:
            gf_port = config.GATEWAY_1_FRONTEND
            gf_name = 'Gateway 1 Frontend'

    # Create the gateway object
    with Pyro4.Daemon(host='localhost', port=gf_port) as daemon:
        gateway_f_obj = Gateway_frontend(gf_name, daemon)
        daemon.requestLoop()
    return


if __name__=='__main__':
    main()
