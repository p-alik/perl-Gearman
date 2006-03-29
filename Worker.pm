#!/usr/bin/perl

#TODO: retries?

use strict;
use Gearman::Util;
use Carp ();
use IO::Socket::INET;

# this is the object that's handed to the worker subrefs
package Gearman::Job;

use fields (
            'func',
            'argref',
            'handle',

            'jss', # job server's socket
            );

sub new {
    my ($class, $func, $argref, $handle, $jss) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{func} = $func;
    $self->{handle} = $handle;
    $self->{argref} = $argref;
    $self->{jss} = $jss;
    return $self;
}

# ->set_status($numerator, $denominator) : $bool_sent_to_jobserver
sub set_status {
    my Gearman::Job $self = shift;
    my ($nu, $de) = @_;

    my $req = Gearman::Util::pack_req_command("work_status",
                                              join("\0", $self->{handle}, $nu, $de));
    return Gearman::Util::send_req($self->{jss}, \$req);
}

sub argref {
    my Gearman::Job $self = shift;
    return $self->{argref};
}

sub arg {
    my Gearman::Job $self = shift;
    return ${ $self->{argref} };
}


package Gearman::Worker;
use Socket qw(IPPROTO_TCP TCP_NODELAY SOL_SOCKET PF_INET SOCK_STREAM);

use fields (
            'job_servers',
            'js_count',
            'sock_cache',        # host:port -> IO::Socket::INET
            'last_connect_fail', # host:port -> unixtime
            'down_since',        # host:port -> unixtime
            'connecting',        # host:port -> unixtime connect started at
            'can',               # func -> subref
            'client_id',         # random identifer string, no whitespace
            );

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers} = [];
    $self->{js_count} = 0;
    $self->{sock_cache} = {};
    $self->{last_connect_fail} = {};
    $self->{down_since} = {};
    $self->{can} = {};
    $self->{client_id} = join("", map { chr(int(rand(26)) + 97) } (1..30));

    $self->job_servers(@{ $opts{job_servers} })
        if $opts{job_servers};

    return $self;
}

sub _get_js_sock {
    my Gearman::Worker $self = shift;
    my $ipport = shift;

    if (my $sock = $self->{sock_cache}{$ipport}) {
        return $sock if getpeername($sock);
        delete $self->{sock_cache}{$ipport};
    }

    my $now = time;
    my $down_since = $self->{down_since}{$ipport};
    if ($down_since) {
        my $down_for = $now - $down_since;
        my $retry_period = $down_for > 60 ? 30 : (int($down_for / 2) + 1);
        if ($self->{last_connect_fail}{$ipport} > $now - $retry_period) {
            return undef;
        }
    }

    return undef unless $ipport =~ /(^\d+\..+):(\d+)/;
    my ($ip, $port) = ($1, $2);

    my $sock = IO::Socket::INET->new(PeerAddr => "$ip:$port",
                                     Timeout => 1);
    unless ($sock) {
        $self->{down_since}{$ipport} ||= $now;
        $self->{last_connect_fail}{$ipport} = $now;
        return undef;
    }
    delete $self->{last_connect_fail}{$ipport};
    delete $self->{down_since}{$ipport};
    $sock->autoflush(1);
    setsockopt($sock, IPPROTO_TCP, TCP_NODELAY, pack("l", 1)) or die;

    $self->{sock_cache}{$ipport} = $sock;

    my $cid_req = Gearman::Util::pack_req_command("set_client_id", $self->{client_id});
    Gearman::Util::send_req($sock, \$cid_req);

    # get this socket's state caught-up
    foreach my $func (keys %{$self->{can}}) {
        unless (_set_capability($sock, $func, 1)) {
            delete $self->{sock_cache}{$ipport};
            return undef;
        }
    }

    return $sock;
}

sub _set_capability {
    my ($sock, $func, $can) = @_;

    my $req = Gearman::Util::pack_req_command($can ? "can_do" : "cant_do",
                                              $func);
    return Gearman::Util::send_req($sock, \$req);
}

# tell all the jobservers that this worker can't do anything
sub reset_abilities {
    my Gearman::Worker $self = shift;
    my $req = Gearman::Util::pack_req_command("reset_abilities");
    foreach my $js (@{ $self->{job_servers} }) {
        my $jss = $self->_get_js_sock($js)
            or next;

        unless (Gearman::Util::send_req($jss, \$req)) {
            delete $self->{sock_cache}{$js};
        }
    }

    $self->{can} = {};
}

# does one job and returns.  no return value.
sub work {
    my Gearman::Worker $self = shift;
    my %opts = @_;
    my $stop_if = delete $opts{'stop_if'} || sub { 0 };
    die "Unknown opts" if %opts;

    my $grab_req = Gearman::Util::pack_req_command("grab_job");
    my $presleep_req = Gearman::Util::pack_req_command("pre_sleep");
    my %fd_map;

    while (1) {

        my @jss;
        my $need_sleep = 1;

        foreach my $js (@{ $self->{job_servers} }) {
            my $jss = $self->_get_js_sock($js)
                or next;

            # TODO: add an optional sleep in here for the test suite
            # to test gearmand server going away here.  (SIGPIPE on
            # send_req, etc) this testing has been done manually, at
            # least.

            unless (Gearman::Util::send_req($jss, \$grab_req) &&
                    Gearman::Util::wait_for_readability($jss->fileno, 0.50)) {
                delete $self->{sock_cache}{$js};
                next;
            }

            my ($res, $err);
            do {
                $res = Gearman::Util::read_res_packet($jss, \$err);
                unless ($res) {
                    delete $self->{sock_cache}{$js};
                    next;
                }
            } while ($res->{type} eq "noop");

            push @jss, [$js, $jss];

            if ($res->{type} eq "no_job") {
                next;
            }

            die "Uh, wasn't expecting a $res->{type} packet" unless $res->{type} eq "job_assign";

            $need_sleep = 0;

            ${ $res->{'blobref'} } =~ s/^(.+?)\0(.+?)\0//
                or die "Uh, regexp on job_assign failed";
            my ($handle, $func) = ($1, $2);
            my $job = Gearman::Job->new($func, $res->{'blobref'}, $handle, $jss);
            my $handler = $self->{can}{$func};
            my $ret = eval { $handler->($job); };

            my $work_req;
            if (defined $ret) {
                $work_req = Gearman::Util::pack_req_command("work_complete", "$handle\0" . (ref $ret ? $$ret : $ret));
            } else {
                $work_req = Gearman::Util::pack_req_command("work_fail", $handle);
            }

            unless (Gearman::Util::send_req($jss, \$work_req)) {
                delete $self->{sock_cache}{$js};
            }
            return;
        }

        if ($need_sleep) {
            my $wake_vec = '';
            foreach my $j (@jss) {
                my ($js, $jss) = @$j;
                unless (Gearman::Util::send_req($jss, \$presleep_req)) {
                    delete $self->{sock_cache}{$js};
                    next;
                }
                my $fd = $jss->fileno;
                vec($wake_vec, $fd, 1) = 1;
            }

            # chill for some arbitrary time until we're woken up again
            select($wake_vec, undef, undef, 10);
        }

        return if $stop_if->();
    }

}

sub register_function {
    my Gearman::Worker $self = shift;
    my $func = shift;
    my $subref = shift;

    my $req = Gearman::Util::pack_req_command("can_do", $func);

    foreach my $js (@{ $self->{job_servers} }) {
        my $jss = $self->_get_js_sock($js)
            or next;

        unless (Gearman::Util::send_req($jss, \$req)) {
            delete $self->{sock_cache}{$js};
        }
    }

    $self->{can}{$func} = $subref;
}

# getter/setter
sub job_servers {
    my Gearman::Worker $self = shift;
    return $self->{job_servers} unless @_;
    my $list = [ @_ ];
    $self->{js_count} = scalar @$list;
    foreach (@$list) {
        $_ .= ":7003" unless /:/;
    }
    return $self->{job_servers} = $list;
}


1;
__END__

=head1 NAME

Gearman::Worker - Worker for gearman distributed job system

=head1 SYNOPSIS

    use Gearman::Worker;
    my $worker = Gearman::Worker->new;
    $worker->job_servers('127.0.0.1');
    $worker->register_function($funcname => $subref);
    $worker->work while 1;

=head1 DESCRIPTION

I<Gearman::Worker> is a worker class for the Gearman distributed job system,
providing a framework for receiving and serving jobs from a Gearman server.

Callers instantiate a I<Gearman::Worker> object, register a list of functions
and capabilities that they can handle, then enter an event loop, waiting
for the server to send jobs.

The worker can send a return value back to the server, which then gets
sent back to the client that requested the job; or it can simply execute
silently.

=head1 USAGE

=head2 Gearman::Worker->new(\%options)

Creates a new I<Gearman::Worker> object, and returns the object.

If I<%options> is provided, initializes the new worker object with the
settings in I<%options>, which can contain:

=over 4

=item * job_servers

Calls I<job_servers> (see below) to initialize the list of job servers.

=back

=head2 $worker->job_servers(@servers)

Initializes the worker I<$worker> with the list of job servers in I<@servers>.
I<@servers> should contain a list of IP addresses, with optional port numbers.
For example:

    $worker->job_servers('127.0.0.1', '192.168.1.100:7003');

If the port number is not provided, 7003 is used as the default.

=head2 $worker->register_function($funcname, $subref)

Registers the function I<$funcname> as being provided by the worker
I<$worker>, and advertises these capabilities to all of the job servers
defined in this worker.

I<$subref> must be a subroutine reference that will be invoked when the
worker receives a request for this function. It will be passed a
I<Gearman::Job> object representing the job that has been received by the
worker.

The subroutine reference can return a return value, which will be sent back
to the job server.

=head2 Gearman::Job->arg

Returns the scalar argument that the client sent to the job server.

=head2 Gearman::Job->set_status($numerator, $denominator)

Updates the status of the job (most likely, a long-running job) and sends
it back to the job server. I<$numerator> and I<$denominator> should
represent the percentage completion of the job.

=head1 EXAMPLES

=head2 Summation

This is an example worker that receives a request to sum up a list of
integers.

    use Gearman::Worker;
    use Storable qw( thaw );
    use List::Util qw( sum );
    my $worker = Gearman::Worker->new;
    $worker->job_servers('127.0.0.1');
    $worker->register_function(sum => sub { sum @{ thaw($_[0]->arg) } });
    $worker->work while 1;

See the I<Gearman::Client> documentation for a sample client sending the
I<sum> job.

=cut
