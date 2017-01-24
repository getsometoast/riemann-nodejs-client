/* initialize our protobuf schema,
   and cache it in memory. */
var riemannSchema;
var eventSchema;
var msgSchema;
var querySchema;
var stateSchema;

if (!riemannSchema) {
  var protobuf  = require('protobufjs');
  var readFile  = require('fs').readFileSync;

  var proto = protobuf.loadSync('./riemann/proto/proto.proto');

  eventSchema = proto.lookup('Event');
  msgSchema = proto.lookup('Msg');
  querySchema = proto.lookup('Query');
  stateSchema = proto.lookup('State');

  var schemaMapping = {
    'Event': eventSchema,
    'Msg': msgSchema,
    'Query': querySchema,
    'State': stateSchema
  };

  var getSchema = function (type) {
    return schemaMapping[type];
  };

  riemannSchema = {
    serialize: function (type, value) {
      var serialized;
      try {
        serialized = getSchema(type).encode(value).finish();
      }
      catch (e) {
        console.log(e);
      }
      return serialized;
    },
    parse: function (type, value) {
      return getSchema(type).decode(value);
    }
  };
}

function _serialize(type, value) {
  return riemannSchema.serialize(type, value);
}

function _deserialize(type, value) {
  return riemannSchema.parse(type, value);
}

/* serialization support for all
   known Riemann protobuf types. */

exports.serializeEvent = function(event) {
  return _serialize('Event', event);
};

exports.deserializeEvent = function(event) {
  return _deserialize('Event', event);
};

exports.serializeMessage = function(message) {
  return _serialize('Msg', message);
};

exports.deserializeMessage = function(message) {
  return _deserialize('Msg', message);
};

exports.serializeQuery = function(query) {
  return _serialize('Query', query);
};

exports.deserializeQuery = function(query) {
  return _deserialize('Query', query);
};

exports.serializeState = function(state) {
  return _serialize('State', state);
};

exports.deserializeState = function(state) {
  return _deserialize('State', state);
};
