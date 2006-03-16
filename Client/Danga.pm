#!/usr/bin/perl

#TODO: fail_after_idle
#TODO: find out what fail_after_idle means in this context

package Gearman::Client::Danga;

use strict;

use IO::Handle;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET);

use fields qw(job_servers);

use Gearman::Objects;
use Gearman::Task;
use Gearman::JobStatus;

sub DEBUGGING () { 1 }

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers} = [];

    $self->job_servers(@{ $opts{job_servers} })
        if $opts{job_servers};

    return $self;
}

# getter/setter
sub job_servers {
    my Gearman::Client::Danga $self = shift;
    
    return $self->{job_servers} unless @_;

    $self->shutdown;
    
    my $list = [];

    foreach (@_) {
        my ($host, $port) = split /:/;
        $port ||= 7003;
        my $server = Gearman::Client::Danga::Socket->new( $host, $port );
        
        push @$list, $server;
    }
    return $self->{job_servers} = $list;
}

sub shutdown {
    my Gearman::Client::Danga $self = shift;
    
    foreach (@{$self->{job_servers}}) {
        $_->close( "Shutdown" );
    }
}

sub add_task {
    my Gearman::Client::Danga $self = shift;
    my Gearman::Task $task = shift;

    my @job_servers = grep { $_->safe } @{$self->{job_servers}};

    if (@job_servers) {
        my $js;
        if (defined( my $hash = $task->hash )) {
            $js = @job_servers[$hash % @job_servers];
        }
        else {
            $js = @job_servers->[int( rand( @job_servers ))];
        }
        $task->{taskset} = $self;
        $js->add_task( $task );
    }
    else {
        $task->fail;
    }    
}


package Gearman::Client::Danga::Socket;


use Danga::Socket;
use base 'Danga::Socket';
use fields qw(state waiting need_handle parser host port to_send safe);

use Gearman::Task;
use Gearman::Util;

use Socket qw(PF_INET IPPROTO_TCP SOCK_STREAM);

sub DEBUGGING () { 1 }

sub new {
    my Gearman::Client::Danga::Socket $self = shift;
    my $host = shift;
    my $port = shift;

    $self = fields::new( $self ) unless ref $self;

    $self->{host} = $host;
    $self->{port} = $port;
    $self->{state} = 'disconnected';
    $self->{waiting} = {};
    $self->{need_handle} = [];
    $self->{to_send} = [];
    $self->{safe} = 1;

    return $self;
}

sub connect {
    my Gearman::Client::Danga::Socket $self = shift;

    $self->{state} = 'connecting';

    my $sock = $self->{sock};
    my $host = $self->{host};
    my $port = $self->{port};

    socket my $sock, PF_INET, SOCK_STREAM, IPPROTO_TCP;
    IO::Handle::blocking($sock, 0);

    unless ($sock && defined fileno($sock)) {
        warn( "Error creating socket: $!\n" );
        return undef;
    }

    $self->SUPER::new( $sock );

    connect $sock, Socket::sockaddr_in( $port, Socket::inet_aton( $host ) );

    $self->{parser} = Gearman::ResponseParser::Danga->new( $self );

    $self->watch_write( 1 );
    $self->watch_read( 1 );
}

sub event_write {
    print "event_write\n";
    my Gearman::Client::Danga::Socket $self = shift;

    if ($self->{state} eq 'connecting') {
        $self->{state} = 'ready';
    }

    my $tasks = $self->{to_send};

    if (@$tasks and $self->{state} eq 'ready') {
        my $task = shift @$tasks;
        $self->write( $task->pack_submit_packet );
        push @{$self->{need_handle}}, $task;
        return;
    }
    
    $self->watch_write( 0 );
}

sub event_read {
    my Gearman::Client::Danga::Socket $self = shift;

    my $input = $self->read( 128 x 1024 );

    if ($input) {
        $self->{parser}->parse_data( $input );
    }
    else {
        $self->close( "EOF" );
    }
}

sub event_err {
    my Gearman::Client::Danga::Socket $self = shift;

    if (DEBUGGING and $self->{state} eq 'connecting') {
        warn "Jobserver, $self->{host}:$self->{port} ($self) has failed to connect properly\n";
    }

    $self->_mark_unsafe;
    $self->close( "error" );
}

sub mark_unsafe {
    my Gearman::Client::Danga::Socket $self = shift;

    $self->{safe} = 0;

    Danga::Socket->AddTimer( 10, sub { $self->{safe} = 1; } );
}

sub close {
    my Gearman::Client::Danga::Socket $self = shift;
    my $reason = shift;
    
    $self->{state} = 'disconnected';
    $self->SUPER::close( $reason );
    $self->_requeue_all;
}

sub safe {
    my Gearman::Client::Danga::Socket $self = shift;
    
    return $self->{safe};
}

sub add_task {
    my Gearman::Client::Danga::Socket $self = shift;
    my Gearman::Task $task = shift;

    if ($self->{state} eq 'disconnected') {
        $self->connect;
    }

    $self->watch_write( 1 );

    push @{$self->{to_send}}, $task;
}

sub _requeue_all {
    my Gearman::Client::Danga::Socket $self = shift;
    
    my $to_send = $self->{to_send};
    my $need_handle = $self->{need_handle};
    my $waiting = $self->{waiting};

    $self->{to_send} = [];
    $self->{need_handle} = [];
    $self->{waiting} = {};
    
    while (@$to_send) {
        my $task = shift @$to_send;
        warn "Task $task in to_send queue during socket error, queueing for redispatch\n" if DEBUGGING;
        $task->{taskset}->add_task( $task );
    }

    while (@$need_handle) {
        my $task = shift @$need_handle;
        warn "Task $task in need_handle queue during socket error, queueing for redispatch\n" if DEBUGGING;
        $task->{taskset}->add_task( $task );
    }

    while (my ($shandle, $task) = each( %$waiting )) {
        warn "Task $task ($shandle) in waiting queue during socket error, queueing for redispatch\n" if DEBUGGING;
        $task->{taskset}->add_task( $task );
    }
}

sub process_packet {
    my Gearman::Client::Danga::Socket $self = shift;
    my $res = shift;

    if ($res->{type} eq "job_created") {
        my Gearman::Task $task = shift @{ $self->{need_handle} } or
            die "Um, got an unexpected job_created notification";

        my $shandle = ${ $res->{'blobref'} };

        # did sock become disconnected in the meantime?
        if ($self->{state} ne 'ready') {
            $self->_fail_jshandle($shandle);
            return 1;
        }

        push @{ $self->{waiting}->{$shandle} ||= [] }, $task;
        return 1;
    }

    if ($res->{type} eq "work_fail") {
        my $shandle = ${ $res->{'blobref'} };
        $self->_fail_jshandle($shandle);
        return 1;
    }

    if ($res->{type} eq "work_complete") {
        ${ $res->{'blobref'} } =~ s/^(.+?)\0//
            or die "Bogus work_complete from server";
        my $shandle = $1;

        my $task_list = $self->{waiting}{$shandle} or
            die "Uhhhh:  got work_complete for unknown handle: $shandle\n";

        my Gearman::Task $task = shift @$task_list or
            die "Uhhhh:  task_list is empty on work_complete for handle $shandle\n";

        $task->complete($res->{'blobref'});
        delete $self->{waiting}{$shandle} unless @$task_list;

        warn "Jobs: " . scalar( keys( %{$self->{waiting}} ) ) . "\n";

        return 1;
    }

    if ($res->{type} eq "work_status") {
        my ($shandle, $nu, $de) = split(/\0/, ${ $res->{'blobref'} });

        my $task_list = $self->{waiting}{$shandle} or
            die "Uhhhh:  got work_status for unknown handle: $shandle\n";

        # FIXME: the server is (probably) sending a work_status packet for each
        # interested client, even if the clients are the same, so probably need
        # to fix the server not to do that.  just put this FIXME here for now,
        # though really it's a server issue.
        foreach my Gearman::Task $task (@$task_list) {
            $task->status($nu, $de);
        }

        return 1;
    }

    die "Unknown/unimplemented packet type: $res->{type}";

}

# note the failure of a task given by its jobserver-specific handle
sub _fail_jshandle {
    my Gearman::Client::Danga::Socket $self = shift;
    my $shandle = shift;

    my $task_list = $self->{waiting}->{$shandle} or
        die "Uhhhh:  got work_fail for unknown handle: $shandle\n";

    my Gearman::Task $task = shift @$task_list or
        die "Uhhhh:  task_list is empty on work_fail for handle $shandle\n";

    $task->fail;
    delete $self->{waiting}{$shandle} unless @$task_list;
}

package Gearman::ResponseParser::Danga;

use strict;
use warnings;

use Gearman::ResponseParser;
use base 'Gearman::ResponseParser';

sub new {
    my $class = shift;

    my $self = $class->SUPER::new;

    $self->{_client} = shift;

    return $self;
}

sub on_packet {
    my $self = shift;
    my $packet = shift;

    $self->{_client}->process_packet( $packet );
}

sub on_error {
    my $self = shift;
    
    $self->{_client}->mark_unsafe;
    $self->{_client}->close;
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

=head2 Gearman::Client->new(%options)

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

=head2 $client-E<gt>do_task($task)

=head2 $client-E<gt>do_task($funcname, $arg, \%options)

Dispatches a task and waits on the results.  May either provide a
L<Gearman::Task> object, or the 3 arguments that the Gearman::Task
constructor takes.

Returns a scalar reference to the result, or undef on failure.

If you provide on_complete and on_fail handlers, they're ignored, as
this function currently overrides them.

=head2 $client-E<gt>dispatch_background($task)

=head2 $client-E<gt>dispatch_background($funcname, $arg, \%options)

Dispatches a task and doesn't wait for the result.  Return value
is an opaque scalar that can be used to refer to the task.

=head2 $taskset = $client-E<gt>new_task_set

Creates and returns a new I<Gearman::Taskset> object.

=head2 $taskset-E<gt>add_task($task)

=head2 $taskset-E<gt>add_task($funcname, $arg, $uniq)

=head2 $taskset-E<gt>add_task($funcname, $arg, \%options)

Adds a task to a taskset.  Three different calling conventions are
available.

=head2 $taskset-E<gt>wait

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
