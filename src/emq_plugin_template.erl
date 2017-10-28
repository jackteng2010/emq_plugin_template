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

%% Called when the plugin application stop
unload() ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
	emqttd:unhook('session.created', fun ?MODULE:on_session_created/3),
	emqttd:unhook('session.terminated', fun ?MODULE:on_session_terminated/4),
	emqttd:unhook('message.publish', fun ?MODULE:on_message_publish/2).

%% Init Kafka function
ekaf_init(_Env) ->
	{ok, KafkaValue} = application:get_env(emq_plugin_template, kafka),
	Broker = proplists:get_value(bootstrap_broker, KafkaValue),
	Topic = proplists:get_value(bootstrap_topic, KafkaValue),
	application:load(ekaf),
	application:set_env(ekaf, ekaf_bootstrap_broker, Broker),
	application:set_env(ekaf, ekaf_bootstrap_topics, Topic),
	{ok, _} = application:ensure_all_started(ekaf),
    io:format("Init kafka with Broker: ~p Topic:~p ~n", [Broker, Topic]).

on_client_connected(ConnAck, Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),
    {ok, Client}.

on_client_disconnected(Reason, _Client = #mqtt_client{client_id = ClientId}, _Env) ->
    io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),
    ok.

on_session_created(ClientId, Username, _Env) ->
    io:format("session(~s/~s) created.", [ClientId, Username]).

on_session_terminated(ClientId, Username, Reason, _Env) ->
    io:format("session(~s/~s) terminated: ~p.", [ClientId, Username, Reason]).

%% transform message and return
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("publish ~s~n", [emqttd_message:format(Message)]),
    {ok, Message}.

