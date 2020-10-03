%without fault tolerance
-module(gms1).
-export([leader/4,slave/5,start/1,start/2]).

 %muticast the message to all the peers and application layer
leader(Id, Master, Slaves, Group) ->
    receive
        %the message that needs to be muticast
       {mcast, Msg} ->
           %send message to peers
        bcast(Id, {msg, Msg}, Slaves),
        %send message to myself
        Master ! Msg,
        leader(Id, Master, Slaves, Group);
    %add a new peer
    {join, Wrk, Peer} ->
        %new peer at the end of the list
        Slaves2 = lists:append(Slaves, [Peer]),
        Group2 = lists:append(Group, [Wrk]),
        bcast(Id, {view, [self()|Slaves2], Group2}, Slaves2),
        Master ! {view, Group2},
%stop leader
leader(Id, Master, Slaves2, Group2);
    stop ->
    ok
end.

%sending message to all peers
bcast(_Id,Msg,Peers)->
     lists:foreach(fun(Peer) -> Peer ! Msg end, Peers).

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
        %message from leader, it is then sent to the application layer
        {msg, Msg} ->
            Master ! Msg,
            slave(Id, Master, Leader, Slaves, Group);
        %from leader
        %the view message is then sent to application layer
        {view, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            slave(Id, Master, Leader, Slaves2, Group2);
        stop ->
            ok
        end.

%initialize a group with only one node which is just itself as the master
start(Id)->
    Self = self(),
    {ok,spawn_link(fun()->init(Id,Self)end)}.

init(Id,Master)->
    leader(Id,Master,[],[Master]).

%start a node to join an existing group
start(Id,Grp)->
    Self = self(),
    {ok,spawn_link(fun()->init(Id,Grp,Self) end)}.

init(Id,Grp,Master)->
    Self = self(),
    Grp!{join,Master,Self},
    receive
        %invitation message
        {view,[Leader|Slaves],Group}->
            Master!{view,Group},
            slave(Id,Master,Leader,Slaves,Group)
        end.