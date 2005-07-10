#!/usr/bin/perl

#TODO: priorities
#TODO: fail_after_idle
#TODO: hashing onto job servers?

package Gearman::Client;

use strict;
use IO::Socket::INET;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET);

use Gearman::Objects;
use Gearman::Task;
use Gearman::Taskset;
use Gearman::JobStatus;

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers} = [];
    $self->{js_count} = 0;
    $self->{sock_cache} = {};

    $self->job_servers(@{ $opts{job_servers} })
        if $opts{job_servers};

    return $self;
}

sub new_task_set {
    my Gearman::Client $self = shift;
    return Gearman::Taskset->new($self);
}

# getter/setter
sub job_servers {
    my Gearman::Client $self = shift;
    return $self->{job_servers} unless @_;
    my $list = [ @_ ];
    $self->{js_count} = scalar @$list;
    foreach (@$list) {
        $_ .= ":7003" unless /:/;
    }
    return $self->{job_servers} = $list;
}

# given a (func, arg_p, opts?), returns either undef (on fail) or scalarref of result
sub do_task {
    my Gearman::Client $self = shift;
    my ($func, $arg_p, $opts) = @_;
    my $argref = ref $arg_p ? $arg_p : \$arg_p;
    Carp::croak("Function argument must be scalar or scalarref")
        unless ref $argref eq "SCALAR";

    my $ret = undef;
    my $did_err = 0;

    $opts ||= {};

    $opts->{on_complete} = sub {
	$ret = shift;
    };

    $opts->{on_fail} = sub {
	$did_err = 1;
    };
    
    my $ts = $self->new_task_set;
    $ts->add_task($func, $arg_p, $opts);
    $ts->wait;

    return $did_err ? undef : $ret;

}

# given a (func, arg_p, uniq)
sub dispatch_background {
    my Gearman::Client $self = shift;
    my ($func, $arg_p, $uniq) = @_;
    my $argref = ref $arg_p ? $arg_p : \$arg_p;
    Carp::croak("Function argument must be scalar or scalarref")
        unless ref $argref eq "SCALAR";
    $uniq ||= "";

    my ($jst, $jss) = $self->_get_random_js_sock;
    return 0 unless $jss;

    my $req = Gearman::Util::pack_req_command("submit_job_bg",
                                              "$func\0$uniq\0$$argref");
    my $len = length($req);
    my $rv = $jss->write($req, $len);
    print "dispatch_background:  len=$len, rv=$rv\n";

    my $err;
    my $res = Gearman::Util::read_res_packet($jss, \$err);
    return 0 unless $res && $res->{type} eq "job_created";
    return "$jst//${$res->{blobref}}";
}

sub get_status {
    my Gearman::Client $self = shift;
    my $handle = shift;
    my ($hostport, $shandle) = split(m!//!, $handle);
    print "  hostport=[$hostport], shandle=[$shandle]\n";
    return undef unless grep { $hostport eq $_ } @{ $self->{job_servers} };

    my $sock = $self->_get_js_sock($hostport)
        or return undef;

    my $req = Gearman::Util::pack_req_command("get_status",
                                              $shandle);
    my $len = length($req);
    my $rv = $sock->write($req, $len);
    print "get_status:  len=$len, rv=$rv\n";

    my $err;
    my $res = Gearman::Util::read_res_packet($sock, \$err);
    return undef unless $res && $res->{type} eq "status_res";
    my @args = split(/\0/, ${ $res->{blobref} });
    return undef unless $args[0];
    shift @args;
    $self->_put_js_sock($hostport, $sock);
    return Gearman::JobStatus->new(@args);
}

# returns a socket from the cache.  it should be returned to the
# cache with _put_js_sock.  the hostport isn't verified. the caller
# should verify that $hostport is in the set of jobservers.
sub _get_js_sock {
    my Gearman::Client $self = shift;
    my $hostport = shift;

    if (my $sock = delete $self->{sock_cache}{$hostport}) {
        return $sock if $sock->connected;
    }

    # TODO: cache, and verify with ->connected
    my $sock = IO::Socket::INET->new(PeerAddr => $hostport,
                                     Timeout => 1)
        or return undef;

    setsockopt($sock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;
    $sock->autoflush(1);
    # TODO: tcp_nodelay?
    return $sock;
}

# way for a caller to give back a socket it previously requested.
# the $hostport isn't verified, so the caller should verify the
# $hostport is still in the set of jobservers.
sub _put_js_sock {
    my Gearman::Client $self = shift;
    my ($hostport, $sock) = @_;

    $self->{sock_cache}{$hostport} ||= $sock;
}

sub _get_random_js_sock {
    my Gearman::Client $self = shift;
    my $getter = shift;
    return undef unless $self->{js_count};

    $getter ||= sub { my $hostport = shift; return $self->_get_js_sock($hostport); };

    my $ridx = int(rand($self->{js_count}));
    for (my $try = 0; $try < $self->{js_count}; $try++) {
        my $aidx = ($ridx + $try) % $self->{js_count};
        my $hostport = $self->{job_servers}[$aidx];
        my $sock = $getter->($hostport) or next;
        return ($hostport, $sock);
    }
    return ();
}

1;
__END__

=head1 NAME

Gearman::Client - Client for gearman distributed job system

=head1 SYNOPSIS

    use Gearman::Client;
    my $client = Gearman::Client->new;
    $client->job_servers('127.0.0.1', '10.0.0.1');

    # running a single task
    my $result_ref = $client->do_task("add", "1+2");
    print "1 + 2 = $$result_ref\n";

    # waiting on a set of tasks in parallel
    my $taskset = $client->new_task_set;
    $taskset->add_task( "add" => "1+2", {
       on_complete => sub { ... } 
    });
    $taskset->add_task( "divide" => "5/0", {
       on_fail => sub { print "divide by zero error!\n"; },
    });
    $taskset->wait;


=head1 DESCRIPTION

I<Gearman::Client> is a client class for the Gearman distributed job
system, providing a framework for sending jobs to one or more Gearman
servers.  These jobs are then distributed out to a farm of workers.

Callers instantiate a I<Gearman::Client> object and from it dispatch
single tasks, sets of tasks, or check on the status of tasks.

=head1 USAGE

=head2 Gearman::Client->new(\%options)

Creates a new I<Gearman::Client> object, and returns the object.

If I<%options> is provided, initializes the new client object with the
settings in I<%options>, which can contain:

=over 4

=item * job_servers

Calls I<job_servers> (see below) to initialize the list of job
servers.  Value in this case should be an arrayref.

=back

=head2 $client->job_servers(@servers)

Initializes the client I<$client> with the list of job servers in I<@servers>.
I<@servers> should contain a list of IP addresses, with optional port
numbers. For example:

    $client->job_servers('127.0.0.1', '192.168.1.100:7003');

If the port number is not provided, C<7003> is used as the default.

=head2 $client->do_task($funcname, $arg, \%options)

Dispatches a task and waits on the results.  I<$funcname> is the name
of the task, I<$arg> is a scalar or scalarref representing the
arguments to pass to the task (an opaque scalar, not interpretted by
the library or server, just your worker).  I<\%options> can be undef,
or contain keys:

=over 4

=item * uniq

A key which indicates to the server that other tasks with the same
function name and key will be merged into one.  That is, the task
will be run just once, but all the listeners waiting on that job
will get the response multiplexed back to them.

Uniq may also contain the magic value "-" (a single hyphen) which
means the uniq key is the contents of the args.

=item * on_status

A subroutine reference to be invoked if the task emits status updates.
Arguments passed to the subref are ($numerator, $denominator), where those
are left up to the client and job to determine.

=item * retry_count

Number of times job will be retried if there are failures.  Defaults to 0.

=item * high_priority

Boolean, whether this job should take priority over other jobs already
enqueued.

=item * fail_after_idle

Automatically fail after this many seconds have elapsed.  Defaults to 0,
which means never.

=back

=head2 $client->new_task_set

Creates and returns a new I<Gearman::Taskset> object.

=head2 Gearman::Taskset->add_task($funcname, $arg, \%options)

Sends a task to the job server. I<$funcname> is the name of the task, and
I<$arg> should be either a scalar of reference to a scalar representing
the arguments for the task.

I<%options> can contain:

=over 4

=item * uniq

=item * on_complete

A subroutine reference to be invoked when the task is completed. The
subroutine will be passed a reference to the return value from the worker
process.

=item * on_fail

A subroutine reference to be invoked when the task fails (or fails for
the last time, if retries were specified).  No arguments are
passed to this callback.  This callback won't be called after a failure
if more retries are still possible.

=item * on_status

A subroutine reference to be invoked if the task emits status updates.
Arguments passed to the subref are ($numerator, $denominator), where those
are left up to the client and job to determine.

=item * retry_count

Number of times job will be retried if there are failures.  Defaults to 0.

=item * high_priority

Boolean, whether this job should take priority over other jobs already
enqueued.

=item * fail_after_idle

Automatically fail after this many seconds have elapsed.  Defaults to 0,
which means never.

=back

=head2 Gearman::Taskset->wait

Waits for a response from the job server for any of the tasks listed
in the taskset. Will call the I<on_*> handlers for each of the tasks
that have been completed, updated, etc.  Doesn't return until
everything has finished running or failing.

=head1 EXAMPLES

=head2 Summation

This is an example client that sends off a request to sum up a list of
integers.

    use Gearman::Client;
    use Storable qw( freeze );
    my $client = Gearman::Client->new;
    $client->job_servers('127.0.0.1');
    my $tasks = $client->new_task_set;
    my $handle = $tasks->add_task(sum => freeze([ 3, 5 ]), {
        on_complete => sub { print ${ $_[0] }, "\n" }
    });
    $tasks->wait;

See the I<Gearman::Worker> documentation for the worker for the I<sum>
function.

=cut
