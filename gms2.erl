%election possible
%the crash of leader might cause out of syn
-module(gms2).
-export([leader/4,slave/5,start/1,start/2]).
-define(arghh, 100).

%muticast the message to all the peers and the master
leader(Id, Master, Slaves, Group) ->
    receive
       {mcast, Msg} ->
           %leader broadcast to the slaves
        bcast(Id, {msg, Msg}, Slaves),
        %leader send to itself
        Master ! Msg,
        leader(Id, Master, Slaves, Group);
    %add a new peer
    {join, Wrk, Peer} ->
        %new peer at the end of the list
        Slaves2 = lists:append(Slaves, [Peer]),
        Group2 = lists:append(Group, [Wrk]),
        %broadcast the view to the slaves(including the new node) and itself
        bcast(Id, {view, [self()|Slaves2], Group2}, Slaves2),
        Master ! {view, Group2},
%stop leader
leader(Id, Master, Slaves2, Group2);
    stop ->
    ok
end.

%sending message to all peers
%bcast(_Id,Msg,Peers)->
%     lists:foreach(fun(Peer) -> Peer ! Msg end, Peers).

%when sending message for all peers, there is possibility for node to crash.
bcast(Id, Msg, Nodes) ->
    lists:foreach(fun(Node) -> Node ! Msg, crash(Id) end, Nodes).

%node crash
crash(Id) ->
    case random:uniform(?arghh) of
    ?arghh ->
        io:format("leader ~w: crash~n", [Id]),
    exit(no_luck);
    _ ->
       ok
end.

%slave behavior
slave(Id, Master, Leader, Slaves, Group) ->
    receive
        %message from application layer, send to leader
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, Slaves, Group);
        %new peer comes, forward the message to the leader
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, Slaves, Group);
        %message from leader, it is then sent to the master
        {msg, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group);
        %from leader
        %the view message is then sent to master
        {view, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2);
        %detect the leader dies
        %start election of new leader
        {'DOWN',_Ref,process,Leader,_Reason}->
            election(Id,Master,Slaves,Group);
        stop ->
            ok
        end.

%initialize a group with only one node
% It considers itself as the master
start(Id)->
    %to ensure that all the process will not crash at the same time
    Rnd = random:uniform(1000),
    Self = self(),
    {ok,spawn_link(fun()->init(Id,Rnd,Self)end)}.

init(Id,Rnd,Master)->
    random:seed(Rnd,Rnd,Rnd),
    leader(Id,Master,[],[Master]).

%start a node to join an existing group
start(Id,Grp)->
    Rnd = random:uniform(1000),
    Self = self(),
    {ok,spawn_link(fun()->init(Id,Grp,Rnd,Self) end)}.

init(Id,Grp,Rnd,Master)->
    random:seed(Rnd,Rnd,Rnd),
    Self = self(),
    Grp!{join,Master,Self},
    receive
        %invitation message
        {view,[Leader|Slaves],Group}->
            Master!{view,Group},
            %monitor the leader process
            erlang:monitor(process,Leader),
            slave(Id,Master,Leader,Slaves,Group)
        %If leader crashes, and there is no reply from leader
        after 3000->
            Master!{error,"no reply from leader"}
        end.

%election process
election(Id,Master,Slaves,[_|Group])->
    Self = self(),
    case Slaves of
        %if the node itself is the first at the list, then it will become the next leader
        [Self|Rest]->
            %broadcast the update view
            bcast(Id,{view,Slaves,Group},Rest),
            Master!{view,Group},
            %start leader process
            leader(Id,Master,Rest,Group);
        %Otherwise, select the first node in the list to be the leader
        [Leader|Rest]->
            %monitor the leader process
            erlang:monitor(process,Leader),
            %start self as a slave
            slave(Id,Master,Leader,Rest,Group)
        end.