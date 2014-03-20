# -*- coding: utf-8 -*-
import traceback

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

from thrift.Thrift import TProcessor, TMessageType
from thrift.protocol.TProtocol import TProtocolBase
from types import *

DEBUG = False

class ThriftClientConfig(object):
	def __init__(self,ip=None,port=None,weight=0):
		self._ip = ip
		self._port= port
		self._weight=weight
	
	@property
	def weight(self):
		return self._weight
	
	@property
	def ip(self):
		return self._ip
		
	@property
	def port(self):
		return self._port

class ThriftPoolConfig(object):
	def __init__(self):
		pass
		
class ThriftClientPool(object):
	def __init__(self,config=None,timeout=5000,clientInterface=None,serviceName=None,protocol=TBinaryProtocol,framed=True):
		#for config in configs :
		if not isinstance(config,ThriftClientConfig):
			raise Exception("config must be object of ThriftClientConfig")
		
		self._config = config
		self._timeout = timeout
		if clientInterface is None :
			raise Exception("clientInterface must not be None")
		self._clientInterface = clientInterface
		self._serviceName = serviceName
		if serviceName is not None :
			self._multiplexed = True
		else :
			self._multiplexed = False
		
		self._protocol = protocol
		if self._protocol is None :
			self._protocol = TBinaryProtocol
		
		self._framed = framed
		self._initPool()
	
	def _initPool(self):
		pass
		
	def __makeObject(self):
		return ThriftClient(self._config.ip,self._config.port,clientInterface=self._clientInterface,serviceName=self._serviceName,protocol=self._protocol,framed=self._framed)
		
	def returnObject(self,thriftClient):
		thriftClient.close()
		
	def brokenObject(self,thriftClient):
		thriftClient.close()
		
	def borrowObject(self):
		return self.__makeObject()
	
	def getClient(self):
		return self._ClientProxy(self)
		
	class _ClientProxy(object):
		def __init__(self,pool):
			self._pool = pool
			
		def __getattr__(self,method):
			client = self._pool.borrowObject()
			return MethodInvoker(self._pool,client,method)
			
	
class MethodInvoker(object):
	def __init__(self,pool,client,method):
		self._pool=pool
		self._client= client
		self._method = method
		
	def __call__(self,*args,**argsmap):
		try :
			func = getattr(self._client,self._method)
			return func(*args,**argsmap)
		except Exception , e:
			self._pool.brokenObject(self._client)
			raise e
		finally :
			self._pool.returnObject(self._client)
		

class ThriftClient(object):
	def __init__(self,ip=None,port=None,timeout=5000,clientInterface=None,serviceName=None,protocol=TBinaryProtocol,framed=True) :
		self._ip = ip
		self._port = port
		self._timeout= timeout
		self._client = clientInterface
		self._clientObj = None
		self._transport = None
		self._serviceName=serviceName
		self._protocol = protocol
		self._framed= framed
		
	def __getattr__(self,method):
		return self._RemoteInterface(self.get_client(),method)

	def get_client(self) :
		if(None != self._clientObj) :
			return self._clientObj
		else :
			if(None != self._client and None != self._client) :
				transport = TSocket.TSocket(self._ip,self._port)
				transport.setTimeout(self._timeout)
				if self._framed :
					transport = TTransport.TFramedTransport(transport)
				protocol = self._protocol(transport)
				if None != self._serviceName:
					protocol = TMultiplexedProtocol(protocol,self._serviceName)
				self._transport = transport
				self.open()
				self._clientObj = self._client(protocol)
				return self._clientObj
			else :
				return None
				

	def open(self) :
		if(None != self._transport) :
			self._transport.open()

	def close(self) :
		if(None != self._transport) :
			self._transport.close()

	def __enter__(self):
		return self

	def __exit__(self,type, value, traceback) :
		self.close()
		
	class _RemoteInterface(object):
		def __init__(self,client,method):
			self._client =client
			self._method =method
			
		def __call__(self,*args,**argmap):
			return getattr(self._client,self._method)(*args,**argmap)

"""
Python port of https://issues.apache.org/jira/secure/attachment/12416581/THRIFT-563.patch
"""

SEPARATOR = ":"

class TMultiplexedProcessor(TProcessor):
	def __init__(self):
		self.services = {}

	def registerProcessor(self, serviceName, processor):
		self.services[serviceName] = processor

	def process(self, iprot, oprot):
		(name, type, seqid) = iprot.readMessageBegin();
		if type != TMessageType.CALL & type != TMessageType.ONEWAY:
			raise TException("This should not have happened!?")

		index = name.find(SEPARATOR)
		if index < 0:
			raise TException("Service name not found in message name: " + message.name + ". Did you forget to use a TMultiplexProtocol in your client?")

		serviceName = name[0:index]
		call = name[index+len(SEPARATOR):]
		if not self.services.has_key(serviceName):
			raise TException("Service name not found: " + serviceName + ". Did you forget to call registerProcessor()?")

		standardMessage = (
			call,
			type,
			seqid
		)
		return self.services[serviceName].process(StoredMessageProtocol(iprot, standardMessage), oprot)

class TProtocolDecorator():
	def __init__(self, protocol):
		TProtocolBase(protocol)
		self.protocol = protocol;
	def __getattr__(self, name):
		if hasattr(self.protocol, name):
			member = getattr(self.protocol, name)
			if type(member) in [MethodType, UnboundMethodType, FunctionType, LambdaType, BuiltinFunctionType, BuiltinMethodType]:
				return lambda *args, **kwargs: self._wrap(member, args, kwargs)
			else:
				return member
		raise AttributeError(name)
	def _wrap(self, func, args, kwargs):
		if type(func) == MethodType:
			result = func(*args, **kwargs)
		else:
			result = func(self.protocol, *args, **kwargs)
		return result
class TMultiplexedProtocol(TProtocolDecorator):
	def __init__(self, protocol, serviceName):
		TProtocolDecorator.__init__(self, protocol)
		self.serviceName = serviceName
	def writeMessageBegin(self, name, type, seqid):
		if (type == TMessageType.CALL or
		    type == TMessageType.ONEWAY):
			self.protocol.writeMessageBegin(
			    self.serviceName + SEPARATOR + name,
			    type,
			    seqid
			)
		else:
			self.protocol.writeMessageBegin(name, type, seqid)
class StoredMessageProtocol(TProtocolDecorator):
	def __init__(self, protocol, messageBegin):
		TProtocolDecorator.__init__(self, protocol)
		self.messageBegin = messageBegin

	def readMessageBegin(self):
		return self.messageBegin

