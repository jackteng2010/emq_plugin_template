%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emq_plugin_template).

-include_lib("emqttd/include/emqttd.hrl").

-export([load/1, unload/0]).

%% Hooks functions

-export([on_client_connected/3, on_client_disconnected/3]).

-export([on_session_created/3, on_session_terminated/4]).

-export([on_message_publish/2]).

%% Called when the plugin application start
load(Env) ->
	ekaf_init([Env]),
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Env]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqttd:hook('session.created', fun ?MODULE:on_session_created/3, [Env]),
    emqttd:hook('session.terminated', fun ?MODULE:on_session_terminated/4, [Env]),
    emqttd:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId, username = Username}, Env) ->
	io:format("client ~s/~s connected, connack: ~w~n", [ClientId, Username, ConnAck]),
    Json = mochijson2:encode([{type, <<"connected">>},
								{clientid, ClientId},
								{username, Username},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId, username = Username}, Env) ->
    io:format("client  ~s/~s disconnected, reason: ~w~n", [ClientId, Username, Reason]),
    Json = mochijson2:encode([{type, <<"disconnected">>},
								{clientid, ClientId},
								{username, Username},
							  	{reason, Reason},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    ok.

on_session_created(ClientId, Username, _Env) ->
	io:format("session(~s/~s) created.~n", [ClientId, Username]),
    Json = mochijson2:encode([{type, <<"session_created">>},
								{clientid, ClientId},
								{username, Username},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json).

on_session_terminated(ClientId, Username, Reason, _Env) ->
	io:format("session(~s/~s) terminated: ~p.~n", [ClientId, Username, Reason]),
    Json = mochijson2:encode([{type, <<"session_terminated">>},
								{clientid, ClientId},
								{username, Username},
							  	{reason, Reason},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json).

on_message_publish(Message = #mqtt_message{from = {ClientId, Username},
                        qos     = Qos,
                        retain  = Retain,
                        dup     = Dup,
                        topic   = Topic,
                        payload = Payload}, _Env) ->
    io:format("publish >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>~n"),
    Json = mochijson2:encode([{type, <<"publish">>},
								{clientid, ClientId},
								{username, Username},
							  	{qos, Qos},
							  	{retain, Retain},
							  	{dup, Dup},
							  	{topic, Topic},
							  	{payload, Payload},
								{ts, emqttd_time:now_secs()}]),
	produce_to_kafka(Json),
    {ok, Message}.

ekaf_init(_Env) ->
	{ok, KafkaValue} = application:get_env(emq_plugin_template, kafka),
	BootstrapBroker = proplists:get_value(bootstrap_broker, KafkaValue),
	BootstrapTopic = proplists:get_value(bootstrap_topic, KafkaValue),
	application:load(ekaf),
	application:set_env(ekaf, ekaf_bootstrap_topics, BootstrapTopic),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
	{ok, _} = application:ensure_all_started(ekaf),
    io:format("Init ekaf with ~p ~p~n", [BootstrapBroker, BootstrapTopic]).
	
%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
    emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
    emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).

%% Internal function, send message to kafka
produce_to_kafka(Json) ->
	{ok, KafkaValue} = application:get_env(emq_plugin_template, kafka),
	BootstrapTopic = proplists:get_value(bootstrap_topic, KafkaValue),
	
	io:format("produce to kafka json ~p~n", [Json]),
%%     try ekaf:produce_sync(BootstrapTopic, <<"foo 123">>)
%%     catch
%%         error:exists -> lager:error("produce_sync error")
%%     end.
	
%% 	ekaf:produce_sync(<<"tech-iot-device-gateway-2040">>, <<"foo 123">>),
	
%% 	io:format("produce to kafka 222 ~p ~n", [BootstrapTopic]),
%% 	ekaf:produce_sync(<<"tech-iot-device-gateway-2040">>, list_to_binary(Json)),
	
%% 	io:format("produce to kafka 333 ~p ~n", [BootstrapTopic]),
%% 	Re = ekaf:produce_async(<<BootstrapTopic>>, list_to_binary(Json)),
	
%% 	io:format("Kafka response ~s~n", [Re]).
	io:format("Kafka response ~n").



