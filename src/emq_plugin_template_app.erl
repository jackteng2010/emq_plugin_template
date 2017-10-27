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

-module(emq_plugin_template_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emq_plugin_template_sup:start_link(),
    ok = emqttd_access_control:register_mod(auth, emq_auth_demo, []),
    ok = emqttd_access_control:register_mod(acl, emq_acl_demo, []),
    emq_plugin_template:load(application:get_all_env()),
	
	{ok, KafkaValue} = application:get_env(?APP, kafka, undefined),
	BootstrapBroker = proplists:get_value(bootstrap_broker, KafkaValue, undefined),
	PartitionStrategy = proplists:get_value(partition_strategy, KafkaValue, undefined),

	io:format(">>>>>Init ekaf KafkaValue ~p~n", [KafkaValue]),
	io:format(">>>>>Init ekaf BootstrapBroker ~p~n", [BootstrapBroker]),
	io:format(">>>>>Init ekaf PartitionStrategy ~p~n", [PartitionStrategy]),
	
    {ok, Sup}.

stop(_State) ->
    ok = emqttd_access_control:unregister_mod(auth, emq_auth_demo),
    ok = emqttd_access_control:unregister_mod(acl, emq_acl_demo),
    emq_plugin_template:unload().
