import grpc
from .proto.graphresponse_pb2 import *
from grpc.beta import implementations as beta_implementations
from grpc.beta import interfaces as beta_interfaces


class BetaDgraphServicer(object):
  def Query(self, request, context):
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

class BetaDgraphStub(object):
  def __init__(self, channel):
    self.channel = channel
    self.unary = self.channel.unary_unary(
        '/protos.Dgraph/Run',
        request_serializer=Request.SerializeToString,
        response_deserializer=Response.FromString,
    )

  def Query(self, *args, **kwargs):
    return self.unary(*args, **kwargs)

  async def aQuery(self, *args, **kwargs):
    return self.unary(*args, **kwargs)

def add_DgraphServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'Query': grpc.unary_unary_rpc_method_handler(
          servicer.Query,
          request_deserializer=Request.FromString,
          response_serializer=Response.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'protos.Dgraph', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
