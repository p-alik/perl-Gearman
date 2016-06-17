package Gearman::Taskset;
$Gearman::Taskset::VERSION = '1.13.001';

use strict;
use warnings;

use Socket;

=head1 NAME

Gearman::Taskset - a taskset in Gearman, from the point of view of a client

=head1 SYNOPSIS

    use Gearman::Client;
    my $client = Gearman::Client->new;

    # waiting on a set of tasks in parallel
    my $ts = $client->new_task_set;
    $ts->add_task( "add" => "1+2", {...});
    $ts->wait();


=head1 DESCRIPTION

Gearman::Taskset is a Gearman::Client's representation of tasks queue t in Gearman

=head1 METHODS

=cut

use fields (

    # { handle => [Task, ...] }
    'waiting',

    # Gearman::Client
    'client',

    # arrayref
    'need_handle',

    # default socket (non-merged requests)
    'default_sock',

    # default socket's ip/port
    'default_sockaddr',

    # { hostport => socket }
    'loaned_sock',

    # bool, if taskset has been cancelled mid-processing
    'cancelled',

    # hookname -> coderef
    'hooks',
);

use Carp ();
use Gearman::Util;
use Gearman::ResponseParser::Taskset;

# i thought about weakening taskset's client, but might be too weak.
use Scalar::Util ();
use Time::HiRes  ();

=head2 new($client)

=cut

sub new {
    my $self   = shift;
    my $client = shift;
    ref($client) eq "Gearman::Client"
        || Carp::croak
        "provided client argument is not a Gearman::Client reference";

    unless (ref $self) {
        $self = fields::new($self);
    }

    $self->{waiting}     = {};
    $self->{need_handle} = [];
    $self->{client}      = $client;
    $self->{loaned_sock} = {};
    $self->{cancelled}   = 0;
    $self->{hooks}       = {};

    return $self;
} ## end sub new

sub DESTROY {
    my Gearman::Taskset $ts = shift;

    # During global cleanup this may be called out of order, and the client my not exist in the taskset.
    return unless $ts->{client};

    if ($ts->{default_sock}) {
        $ts->{client}
            ->_put_js_sock($ts->{default_sockaddr}, $ts->{default_sock});
    }

    while (my ($hp, $sock) = each %{ $ts->{loaned_sock} }) {
        $ts->{client}->_put_js_sock($hp, $sock);
    }
} ## end sub DESTROY

=head2 run_hook($name)

=cut

sub run_hook {
    my Gearman::Taskset $self = shift;
    my $name = shift;
    ($name && $self->{hooks}->{$name}) || return;

    eval { $self->{hooks}->{$name}->(@_) };

    warn "Gearman::Taskset hook '$name' threw error: $@\n" if $@;
} ## end sub run_hook

=head2 add_hook($name)

=cut

sub add_hook {
    my Gearman::Taskset $self = shift;
    my $name = shift || return;

    if (@_) {
        $self->{hooks}->{$name} = shift;
    }
    else {
        delete $self->{hooks}->{$name};
    }
} ## end sub add_hook

=head2 client ()

this method is part of the "Taskset" interface, also implemented by
Gearman::Client::Async, where no tasksets make sense, so instead the
Gearman::Client::Async object itself is also its taskset.  (the
client tracks all tasks).  so don't change this, without being aware
of Gearman::Client::Async.  similarly, don't access $ts->{client} without
going via this accessor.

=cut

sub client {
    my Gearman::Taskset $ts = shift;
    return $ts->{client};
}

=head2 cancel()

=cut

sub cancel {
    my Gearman::Taskset $ts = shift;

    $ts->{cancelled} = 1;

    if ($ts->{default_sock}) {
        close($ts->{default_sock});
        $ts->{default_sock} = undef;
    }

    while (my ($hp, $sock) = each %{ $ts->{loaned_sock} }) {
        $sock->close;
    }

    $ts->{waiting}     = {};
    $ts->{need_handle} = [];
    $ts->{client}      = undef;
} ## end sub cancel

#=head2 _get_loaned_sock()
#
#=cut

sub _get_loaned_sock {
    my Gearman::Taskset $ts = shift;
    my $hostport = shift;
    if (my $sock = $ts->{loaned_sock}{$hostport}) {
        return $sock if $sock->connected;
        delete $ts->{loaned_sock}{$hostport};
    }

    my $sock = $ts->{client}->_get_js_sock($hostport);
    return $ts->{loaned_sock}{$hostport} = $sock;
} ## end sub _get_loaned_sock

=head2 wait()

event loop for reading in replies

=cut

sub wait {
    my Gearman::Taskset $ts = shift;
    my %opts = @_;

    my $timeout;
    if (exists $opts{timeout}) {
        $timeout = delete $opts{timeout};
        $timeout += Time::HiRes::time() if defined $timeout;
    }

    Carp::carp "Unknown options: "
        . join(',', keys %opts)
        . " passed to Taskset->wait."
        if keys %opts;

    my %parser;    # fd -> Gearman::ResponseParser object

    my ($rin, $rout, $eout) = ('', '', '');
    my %watching;

    for my $sock ($ts->{default_sock}, values %{ $ts->{loaned_sock} }) {
        next unless $sock;
        my $fd = $sock->fileno;
        vec($rin, $fd, 1) = 1;
        $watching{$fd} = $sock;
    } ## end for my $sock ($ts->{default_sock...})

    my $tries = 0;
    while (!$ts->{cancelled} && keys %{ $ts->{waiting} }) {
        $tries++;

        my $time_left = $timeout ? $timeout - Time::HiRes::time() : 0.5;
        my $nfound = select($rout = $rin, undef, $eout = $rin, $time_left)
            ;    # TODO drop the eout.
        if ($timeout && $time_left <= 0) {
            $ts->cancel;
            return;
        }
        next if !$nfound;

        foreach my $fd (keys %watching) {
            next unless vec($rout, $fd, 1);

            # TODO: deal with error vector

            my $sock = $watching{$fd};
            my $parser = $parser{$fd} ||= Gearman::ResponseParser::Taskset->new(
                source  => $sock,
                taskset => $ts
            );
            eval { $parser->parse_sock($sock); };

            if ($@) {

                # TODO this should remove the fd from the list, and reassign any tasks to other jobserver, or bail.
                # We're not in an accessible place here, so if all job servers fail we must die to prevent hanging.
                Carp::croak("Job server failure: $@");
            } ## end if ($@)
        } ## end foreach my $fd (keys %watching)

    } ## end while (!$ts->{cancelled} ...)
} ## end sub wait

=head2 add_task(Gearman::Task)

=head2 add_task($func, <$scalar | $scalarref>, <$uniq | $opts_hr>

C<$opts_hr>:

=over

=item 

uniq

=item

on_complete

=item

on_fail

=item

on_status

=item

retry_count

=item

fail_after_idle

=item

high_priority

=back

=cut

sub add_task {
    my Gearman::Taskset $ts = shift;
    my $task = $ts->client()->_get_task_from_args(@_);

    $task->taskset($ts);

    $ts->run_hook('add_task', $ts, $task);

    my $jssock = $task->{jssock};

    return $task->fail unless ($jssock);

    my $req = $task->pack_submit_packet($ts->client);
    my $len = length($req);
    my $rv  = $jssock->syswrite($req, $len);
    Carp::croak "Wrote $rv but expected to write $len" unless $rv == $len;

    push @{ $ts->{need_handle} }, $task;
    while (@{ $ts->{need_handle} }) {
        my $rv
            = $ts->_wait_for_packet($jssock, $ts->{client}->{command_timeout});
        if (!$rv) {
            shift @{ $ts->{need_handle} }; # ditch it, it failed.
                                           # this will resubmit it if it failed.
            return $task->fail;
        }
    } ## end while (@{ $ts->{need_handle...}})

    return $task->handle;
} ## end sub add_task

#
# _get_default_soc()
# used in Gearman::Task->taskset only
#
sub _get_default_sock {
    my Gearman::Taskset $ts = shift;
    return $ts->{default_sock} if $ts->{default_sock};

    my $getter = sub {
        my $hostport = shift;
        return $ts->{loaned_sock}{$hostport}
            || $ts->{client}->_get_js_sock($hostport);
    };

    my ($jst, $jss) = $ts->{client}->_get_random_js_sock($getter);
    return unless $jss;
    $ts->{loaned_sock}{$jst} ||= $jss;

    $ts->{default_sock}     = $jss;
    $ts->{default_sockaddr} = $jst;
    return $jss;
} ## end sub _get_default_sock

#
#  _get_hashed_sock($hv)
#
# only used in Gearman::Task->taskset only
#
# return a socket
sub _get_hashed_sock {
    my Gearman::Taskset $ts = shift;
    my $hv = shift;

    my $cl = $ts->client;
    my $sock;
    for (my $off = 0; $off < $cl->{js_count}; $off++) {
        my $idx = ($hv + $off) % ($cl->{js_count});
        $sock = $ts->_get_loaned_sock($cl->{job_servers}[$idx]);
        last;
    }

    return $sock;
} ## end sub _get_hashed_sock

#
#  _wait_for_packet($sock, $timeout)
#
# returns boolean when given a sock to wait on.
# otherwise, return value is undefined.
sub _wait_for_packet {
    my Gearman::Taskset $ts = shift;
    my $sock                = shift;    # socket to singularly read from
    my $timeout             = shift;

    my ($res, $err);
    $res = Gearman::Util::read_res_packet($sock, \$err, $timeout);
    return 0 unless $res;
    return $ts->_process_packet($res, $sock);
} ## end sub _wait_for_packet

#
# _is_port($sock)
#
sub _ip_port {
    my ($self, $sock) = @_;
    return undef unless $sock;
    my $pn = getpeername($sock) or return undef;
    my ($port, $iaddr) = Socket::sockaddr_in($pn);

    return join ':', Socket::inet_ntoa($iaddr), $port;
} ## end sub _ip_port

#
# _fail_jshandle($shandle)
#
# note the failure of a task given by its jobserver-specific handle
#
sub _fail_jshandle {
    my Gearman::Taskset $ts = shift;
    my $shandle = shift;
    $shandle
        or Carp::croak sprintf
        "_fail_jshandle() called without shandle parameter";

    my $task_list = $ts->{waiting}{$shandle}
        or Carp::croak "Uhhhh:  got work_fail for unknown handle: $shandle";

    my $task = shift @$task_list;
    ($task && ref($task) eq "Gearman::Task")
        or Carp::croak
        "Uhhhh:  task_list is empty on work_fail for handle $shandle\n";

    $task->fail;
    delete $ts->{waiting}{$shandle} unless @$task_list;
} ## end sub _fail_jshandle

#
# _process_packet($res, $sock)
#
sub _process_packet {
    my Gearman::Taskset $ts = shift;
    my ($res, $sock) = @_;

    if ($res->{type} eq "job_created") {
        my $task = shift @{ $ts->{need_handle} };
        ($task && ref($task) eq "Gearman::Task")
            or Carp::croak "Um, got an unexpected job_created notification";

        my $shandle = ${ $res->{'blobref'} };
        my $ipport  = $ts->_ip_port($sock);

        # did sock become disconnected in the meantime?
        if (!$ipport) {
            $ts->_fail_jshandle($shandle);
            return 1;
        }

        $task->handle("$ipport//$shandle");
        return 1 if $task->{background};
        push @{ $ts->{waiting}{$shandle} ||= [] }, $task;
        return 1;
    } ## end if ($res->{type} eq "job_created")

    if ($res->{type} eq "work_fail") {
        my $shandle = ${ $res->{'blobref'} };
        $ts->_fail_jshandle($shandle);
        return 1;
    }

    if ($res->{type} eq "work_complete") {
        ${ $res->{'blobref'} } =~ s/^(.+?)\0//
            or Carp::croak "Bogus work_complete from server";
        my $shandle = $1;

        my $task_list = $ts->{waiting}{$shandle}
            or Carp::croak
            "Uhhhh:  got work_complete for unknown handle: $shandle\n";

        my $task = shift @$task_list;
        ($task && ref($task) eq "Gearman::Task")
            or Carp::croak
            "Uhhhh:  task_list is empty on work_complete for handle $shandle\n";

        $task->complete($res->{'blobref'});
        delete $ts->{waiting}{$shandle} unless @$task_list;

        return 1;
    } ## end if ($res->{type} eq "work_complete")

    if ($res->{type} eq "work_exception") {
        ${ $res->{'blobref'} } =~ s/^(.+?)\0//
            or Carp::croak "Bogus work_exception from server";
        my $shandle   = $1;
        my $task_list = $ts->{waiting}{$shandle}
            or Carp::croak
            "Uhhhh:  got work_exception for unknown handle: $shandle\n";

        my $task = $task_list->[0];
        ($task && ref($task) eq "Gearman::Task")
            or Carp::croak
            "Uhhhh:  task_list is empty on work_exception for handle $shandle\n";

        $task->exception($res->{'blobref'});

        return 1;
    } ## end if ($res->{type} eq "work_exception")

    if ($res->{type} eq "work_status") {
        my ($shandle, $nu, $de) = split(/\0/, ${ $res->{'blobref'} });

        my $task_list = $ts->{waiting}{$shandle}
            or Carp::croak
            "Uhhhh:  got work_status for unknown handle: $shandle\n";

        # FIXME: the server is (probably) sending a work_status packet for each
        # interested client, even if the clients are the same, so probably need
        # to fix the server not to do that.  just put this FIXME here for now,
        # though really it's a server issue.
        foreach my $task (@$task_list) {
            $task->status($nu, $de);
        }

        return 1;
    } ## end if ($res->{type} eq "work_status")

    Carp::croak
        "Unknown/unimplemented packet type: $res->{type} [${$res->{blobref}}]";

} ## end sub _process_packet

1;
