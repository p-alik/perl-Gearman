package Gearman::Taskset;
use version;
$Gearman::Taskset::VERSION = qv("2.001.001"); # TRIAL

use strict;
use warnings;

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

use Carp          ();
use Gearman::Util ();
use Gearman::ResponseParser::Taskset;

# i thought about weakening taskset's client, but might be too weak.
use Scalar::Util ();
use Socket       ();
use Time::HiRes ();

=head2 new($client)

=cut

sub new {
    my ($self, $client) = @_;
    (Scalar::Util::blessed($client) && $client->isa("Gearman::Client"))
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
    my $self = shift;

    # During global cleanup this may be called out of order, and the client my not exist in the taskset.
    return unless $self->{client};

    if ($self->{default_sock}) {
        $self->{client}
            ->_put_js_sock($self->{default_sockaddr}, $self->{default_sock});
    }

    while (my ($hp, $sock) = each %{ $self->{loaned_sock} }) {
        $self->{client}->_put_js_sock($hp, $sock);
    }
} ## end sub DESTROY

=head2 run_hook($name)

run a hook callback if defined

=cut

sub run_hook {
    my ($self, $name) = (shift, shift);
    ($name && $self->{hooks}->{$name}) || return;

    eval { $self->{hooks}->{$name}->(@_) };

    warn "Gearman::Taskset hook '$name' threw error: $@\n" if $@;
} ## end sub run_hook

=head2 add_hook($name, [$cb])

add a hook

=cut

sub add_hook {
    my ($self, $name, $cb) = @_;
    $name || return;

    if ($cb) {
        $self->{hooks}->{$name} = $cb;
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
    return shift->{client};
}

=head2 cancel()

=cut

sub cancel {
    my $self = shift;

    $self->{cancelled} = 1;

    if ($self->{default_sock}) {
        close($self->{default_sock});
        $self->{default_sock} = undef;
    }

    while (my ($hp, $sock) = each %{ $self->{loaned_sock} }) {
        $sock->close;
    }

    $self->{waiting}     = {};
    $self->{need_handle} = [];
    $self->{client}      = undef;
} ## end sub cancel

#=head2 _get_loaned_sock($hostport)
#
#=cut

sub _get_loaned_sock {
    my ($self, $hostport) = @_;

    if (my $sock = $self->{loaned_sock}{$hostport}) {
        return $sock if $sock->connected;
        delete $self->{loaned_sock}{$hostport};
    }

    my $sock = $self->{client}->_get_js_sock($hostport);
    return $self->{loaned_sock}{$hostport} = $sock;
} ## end sub _get_loaned_sock

=head2 wait(%opts)

event loop for reading in replies

=cut

sub wait {
    my ($self, %opts) = @_;
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

    for my $sock ($self->{default_sock}, values %{ $self->{loaned_sock} }) {
        next unless $sock;
        my $fd = $sock->fileno;
        vec($rin, $fd, 1) = 1;
        $watching{$fd} = $sock;
    } ## end for my $sock ($self->{default_sock...})

    my $tries = 0;
    while (!$self->{cancelled} && keys %{ $self->{waiting} }) {
        $tries++;

        my $time_left = $timeout ? $timeout - Time::HiRes::time() : 0.5;
        my $nfound = select($rout = $rin, undef, $eout = $rin, $time_left)
            ;    # TODO drop the eout.
        if ($timeout && $time_left <= 0) {
            $self->cancel;
            return;
        }
        next if !$nfound;

        foreach my $fd (keys %watching) {
            next unless vec($rout, $fd, 1);

            # TODO: deal with error vector

            my $sock   = $watching{$fd};
            my $parser = $parser{$fd}
                ||= Gearman::ResponseParser::Taskset->new(
                source  => $sock,
                taskset => $self
                );
            eval { $parser->parse_sock($sock); };

            if ($@) {

                # TODO this should remove the fd from the list, and reassign any tasks to other jobserver, or bail.
                # We're not in an accessible place here, so if all job servers fail we must die to prevent hanging.
                Carp::croak("Job server failure: $@");
            } ## end if ($@)
        } ## end foreach my $fd (keys %watching)

    } ## end while (!$self->{cancelled...})
} ## end sub wait

=head2 add_task(Gearman::Task)

=head2 add_task($func, <$scalar | $scalarref>, <$uniq | $opts_hr>

C<$opts_hr> see L<Gearman::Task>

=cut

sub add_task {
    my $self = shift;
    my $task = $self->client()->_get_task_from_args(@_);

    $task->taskset($self);

    $self->run_hook('add_task', $self, $task);

    my $jssock = $task->{jssock};

    return $task->fail("undefined jssock") unless ($jssock);

    my $req = $task->pack_submit_packet($self->client);
    my $len = length($req);
    my $rv  = $jssock->syswrite($req, $len);
    $rv ||= 0;
    Carp::croak "Wrote $rv but expected to write $len" unless $rv == $len;

    push @{ $self->{need_handle} }, $task;
    while (@{ $self->{need_handle} }) {
        my $rv
            = $self->_wait_for_packet($jssock,
            $self->{client}->{command_timeout});
        if (!$rv) {
            # ditch it, it failed.
            # this will resubmit it if it failed.
            shift @{ $self->{need_handle} };
            return $task->fail(
                join(' ',
                    "no rv on waiting for packet",
                    defined($rv) ? $rv : $!)
            );
        } ## end if (!$rv)
    } ## end while (@{ $self->{need_handle...}})

    return $task->handle;
} ## end sub add_task

#
# _get_default_sock()
# used in Gearman::Task->taskset only
#
sub _get_default_sock {
    my $self = shift;
    return $self->{default_sock} if $self->{default_sock};

    my $getter = sub {
        my $hostport = shift;
        return $self->{loaned_sock}{$hostport}
            || $self->{client}->_get_js_sock($hostport);
    };

    my ($jst, $jss) = $self->{client}->_get_random_js_sock($getter);
    return unless $jss;
    $self->{loaned_sock}{$jst} ||= $jss;

    $self->{default_sock}     = $jss;
    $self->{default_sockaddr} = $jst;

    return $jss;
} ## end sub _get_default_sock

#
#  _get_hashed_sock($hv)
#
# only used in Gearman::Task->taskset only
#
# return a socket
sub _get_hashed_sock {
    my $self = shift;
    my $hv   = shift;

    my $cl = $self->client;
    my $sock;
    for (my $off = 0; $off < $cl->{js_count}; $off++) {
        my $idx = ($hv + $off) % ($cl->{js_count});
        $sock = $self->_get_loaned_sock($cl->{job_servers}[$idx]);
        last;
    }

    return $sock;
} ## end sub _get_hashed_sock

#
#  _wait_for_packet($sock, $timeout)
#
# $sock socket to singularly read from
#
# returns boolean when given a sock to wait on.
# otherwise, return value is undefined.
sub _wait_for_packet {
    my ($self, $sock, $timeout) = @_;

    #TODO check $err after read
    my $err;
    my $res = Gearman::Util::read_res_packet($sock, \$err, $timeout);

    return $res ? $self->process_packet($res, $sock) : 0;
} ## end sub _wait_for_packet

#
# _is_port($sock)
#
# return hostport || ipport
#
sub _ip_port {
    my ($self, $sock) = @_;
    $sock || return;

    my $pn = getpeername($sock);
    $pn || return;

    # look for a hostport in loaned_sock
    my $hostport;
    while (my ($hp, $s) = each %{ $self->{loaned_sock} }) {
        $s || next;
        if ($sock == $s) {
            $hostport = $hp;
            last;
        }
    } ## end while (my ($hp, $s) = each...)

    # hopefully it solves client->get_status mismatch
    $hostport && return $hostport;

    my $fam = Socket::sockaddr_family($pn);
    my ($port, $iaddr)
        = ($fam == Socket::AF_INET6)
        ? Socket::sockaddr_in6($pn)
        : Socket::sockaddr_in($pn);

    my $addr = Socket::inet_ntop($fam, $iaddr);

    return join ':', $addr, $port;
} ## end sub _ip_port

#
# _fail_jshandle($shandle)
#
# note the failure of a task given by its jobserver-specific handle
#
sub _fail_jshandle {
    my ($self, $shandle) = @_;
    $shandle
        or Carp::croak "_fail_jshandle() called without shandle parameter";

    my $task_list = $self->{waiting}{$shandle}
        or Carp::croak "Uhhhh:  got work_fail for unknown handle: $shandle";

    my $task = shift @$task_list;
    ($task && ref($task) eq "Gearman::Task")
        or Carp::croak
        "Uhhhh:  task_list is empty on work_fail for handle $shandle\n";

    $task->fail("jshandle fail");
    delete $self->{waiting}{$shandle} unless @$task_list;
} ## end sub _fail_jshandle

=head2 process_packet($res, $sock)

=cut

sub process_packet {
    my ($self, $res, $sock) = @_;

    if ($res->{type} eq "job_created") {
        my $task = shift @{ $self->{need_handle} };
        ($task && ref($task) eq "Gearman::Task")
            or Carp::croak "Um, got an unexpected job_created notification";
        my $shandle = ${ $res->{'blobref'} };
        my $ipport  = $self->_ip_port($sock);

        # did sock become disconnected in the meantime?
        if (!$ipport) {
            $self->_fail_jshandle($shandle);
            return 1;
        }

        $task->handle("$ipport//$shandle");
        return 1 if $task->{background};
        push @{ $self->{waiting}{$shandle} ||= [] }, $task;
        return 1;
    } ## end if ($res->{type} eq "job_created")

    if ($res->{type} eq "work_fail") {
        my $shandle = ${ $res->{'blobref'} };
        $self->_fail_jshandle($shandle);
        return 1;
    }

    my $qr = qr/(.+?)\0/;

    if ($res->{type} eq "work_complete") {
        (${ $res->{'blobref'} } =~ /^$qr/)
            or Carp::croak "Bogus work_complete from server";
        ${ $res->{'blobref'} } =~ s/^$qr//;
        my $shandle = $1;

        my $task_list = $self->{waiting}{$shandle}
            or Carp::croak
            "Uhhhh:  got work_complete for unknown handle: $shandle\n";

        my $task = shift @$task_list;
        ($task && ref($task) eq "Gearman::Task")
            or Carp::croak
            "Uhhhh:  task_list is empty on work_complete for handle $shandle\n";

        $task->complete($res->{'blobref'});
        delete $self->{waiting}{$shandle} unless @$task_list;

        return 1;
    } ## end if ($res->{type} eq "work_complete")

    if ($res->{type} eq "work_exception") {

        # ${ $res->{'blobref'} } =~ s/^(.+?)\0//
        #     or Carp::croak "Bogus work_exception from server";

        (${ $res->{'blobref'} } =~ /^$qr/)
            or Carp::croak "Bogus work_exception from server";
        ${ $res->{'blobref'} } =~ s/^$qr//;
        my $shandle = $1;

        my $task_list = $self->{waiting}{$shandle}
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

        my $task_list = $self->{waiting}{$shandle}
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
} ## end sub process_packet

1;
