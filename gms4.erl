%not completed
%the node might lost messagess

-module(gms4).

-export([start/1,start/2]).


-define(arghh, 100).

%muticast the message to all the peers and the master
leader(Id, Master, N, Slaves, Group,MsgList) ->
    receive
       {mcast, Msg} ->
        %leader broadcast to the slaves
        bcast(Id, {msg, N, Msg}, Slaves),

        NewMsgList = lists:append(MsgList,[{msg,N,Msg}]),
        %leader send to itself
        Master ! Msg,
        %sequence number plus 1
        leader(Id, Master,N+1, Slaves, Group,NewMsgList);
    %add a new peer
    {join, Wrk, Peer} ->
        %new peer at the end of the list
        Slaves2 = lists:append(Slaves, [Peer]),
        Group2 = lists:append(Group, [Wrk]),
        %broadcast the view to the slaves(including the new node) and itself
        bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),

        NewMsgList2 = lists:append(MsgList,[{view, N, [self()|Slaves2], Group2}]),
        Master ! {view, Group2},
        %sequence number plus 1
        leader(Id, Master,N+1, Slaves2, Group2,NewMsgList2);
    %the slave ask for lost message
    {resend,Num,Slave}->
        LosMsg = list:keyfind(Num,2,MsgList),
        case LosMsg of
            {_,_,_}->
                  Slave ! {resendMsgs,LosMsg};
            false->
                io:format("no such msg in the list.~n")
            end,
      
        leader(Id, Master,N, Slaves, Group,MsgList);
    %stop leader
    stop ->
        ok;
    Error->
            io:format("slave ~w:~w~n",[Id,Error])
end.

%sending message to all peers
%bcast(_Id,Msg,Peers)->
%     lists:foreach(fun(Peer) -> Peer ! Msg end, Peers).

%simluate the lost of message
bcast(_Id,Msg,Peers)->
    lists:foreach(fun(Peer)->
        case random:uniform(?arghh) of
    ?arghh ->
        io:format("peer ~w: message lost~n", [Peer]);
    _ ->
        Peer!Msg
end
end, Peers).

%when sending message for all peers, there is possibility for node to crash.
%bcast(Id, Msg, Nodes) ->
%    lists:foreach(fun(Node) -> Node ! Msg, crash(Id) end, Nodes).




%node crash
%crash(Id) ->
%    case random:uniform(?arghh) of
%    ?arghh ->
%        io:format("leader ~w: crash~n", [Id]),
%    exit(no_luck);
%    _ ->
%        ok
%end.

%slave behavior
%Last is the copy of the last message from leader
slave(Id, Master, Leader, N, Last, Slaves, Group) ->
    receive
        %message from application layer, send to leader
        {mcast, Msg} ->
            Leader ! {mcast, Msg},
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        %new peer comes, forward the message to the leader
        {join, Wrk, Peer} ->
            Leader ! {join, Wrk, Peer},
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        
         %receive old message from leader
        {msg, I, _} when I < N ->
            slave(Id, Master, Leader, N, Last, Slaves, Group);
        
        %receive the message that the server sends again
        {resendMsgs, ReMsg}->
            case ReMsg of
                {msg, _, M}->
                    Master ! M;
                {view,_,_,G}->
                    Master!{view,G}
                end,
            
            slave(Id, Master, Leader, N, ReMsg, Slaves, Group);

        %the slave lost some message
        %the seq number is larger than expected
        {msg,I,Msg} when I > N->
            LackNum = lists:seq(N,I),
            lists:foreach(fun(Num) -> Leader ! {resend, Num, self()}  end, LackNum),
            Master ! Msg,
            slave(Id, Master, Leader, I+1, Last, Slaves, Group);

        %message from leader, it is then sent to the master
        {msg, I, Msg} ->
            Master ! Msg,
            %Last is the copy of the last message
            %sequence number plus 1
            slave(Id, Master, Leader, N+1, {msg, I, Msg}, Slaves, Group);
        %from leader
        %the view message is then sent to master
        {view, I, [Leader|Slaves2], Group2} ->
            Master ! {view, Group2},
            %Last is the copy of the last message
            %sequence number plus 1
            slave(Id, Master, Leader, N+1,{view, I, [Leader|Slaves2], Group2}, Slaves2, Group2);
        %detect the leader dies
        %start election of new leader
        {'DOWN',_Ref,process,Leader,_Reason}->
            election(Id, Master, N, Last, Slaves, Group);
       
        stop ->
            ok;
        Error->
            io:format("slave ~w:~w~n",[Id,Error])
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
    leader(Id,Master,1,[],[Master],[]).

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
        {view,N,[Leader|Slaves],Group}->
            Master!{view, Group},
            %monitor the leader process
            erlang:monitor(process,Leader),
            slave(Id, Master, Leader, N+1,{view, N, [Leader|Slaves],Group}, Slaves, Group)
        %If leader crashes, and there is no reply from leader
        after 3000->
            Master!{error,"no reply from leader"}
        end.

%election process
election(Id,Master, N, Last,Slaves,[_|Group])->
    Self = self(),
    case Slaves of
        %if the node itself is the first at the list, then it will become the next leader
        [Self|Rest]->

            %to send the last message again as the new leader!!!
            bcast(Id,Last,Rest),
            %broadcast the updated view
            bcast(Id,{view,N,Slaves,Group},Rest),
            Master!{view,Group},
            %start self as a leader
            leader(Id,Master,N+1,Rest,Group,[]);
        %Otherwise, select the first node in the list to be the leader
        [Leader|Rest]->
            %monitor the leader process
            erlang:monitor(process,Leader),
            %start self as a slave
            slave(Id,Master,Leader,N,Last,Rest,Group)
        end.

