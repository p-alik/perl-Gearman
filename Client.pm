#!/usr/bin/perl

#TODO: TCP_NODELAY
#TODO: priorities

use strict;
use Gearman::Util;
use Carp ();
use IO::Socket::INET;

package Gearman::Client;
use fields (
            'job_servers',
            'js_count',
            );

package Gearman::JobStatus;

sub new {
    my ($class, $known, $running, $nu, $de) = @_;
    undef $nu unless length($nu);
    undef $de unless length($de);
    my $self = [ $known, $running, $nu, $de ];
    bless $self;
    return $self;
}

sub known { my $self = shift; return $self->[0]; }
sub running { my $self = shift; return $self->[1]; }
sub progress { my $self = shift; return defined $self->[2] ? [ $self->[2], $self->[3] ] : undef; }
sub percent { my $self = shift; return (defined $self->[2] && $self->[3]) ? ($self->[2] / $self->[3]) : undef; }

package Gearman::Taskset;

use fields (
            'waiting',  # { handle => }
            'sock',     # socket
            'sockaddr', # socket's IP:
            'client',
            'need_handle',  # arrayref
            );


package Gearman::Task;

use fields (
            # from client:
            'func',
            'argref',
            # opts from client:
            'uniq',
            'on_complete',
            'on_fail',
            'on_status',
            'retry_count',
            'fail_after_idle',
            'high_priority',

            # from server:
            'handle',

            # maintained by this module:
            'retries_done',
            'taskset',
            );

# ->new(Gearman::Taskset, $func, $argref, $opts);
sub new {
    my $class = shift;

    my $self = $class;
    $self = fields::new($class) unless ref $self;

    my $ts = shift;
    $self->{func} = shift;
    $self->{argref} = shift;

    my $opts = shift || {};
    for my $k (qw( uniq on_complete on_fail on_status retry_count fail_after_idle high_priority )) {
        $self->{$k} = delete $opts->{$k};
    }


    if (%{$opts}) {
        Carp::croak("Unknown option(s): " . join(", ", sort keys %$opts));
    }

    $self->{retries_done} = 0;
    $self->{taskset} = $ts;
    return $self;
}

sub submit_job_args_ref {
    my Gearman::Task $task = shift;
    return \ join("\0", $task->{func}, $task->{uniq}, ${ $task->{argref} });
}

sub fail {
    my Gearman::Task $task = shift;

    # try to retry, if we can
    if ($task->{retries_done} < $task->{retry_count}) {
        $task->{retries_done}++;
        print "retry:  $task->{retries_done} <= $task->{retry_count}\n";
        $task->handle(undef);
        $task->{taskset}->add_task($task);
        return;
    }

    return unless $task->{on_fail};
    $task->{on_fail}->();
}

sub complete {
    my Gearman::Task $task = shift;
    return unless $task->{on_complete};
    my $result_ref = shift;
    $task->{on_complete}->($result_ref);
}

sub status {
    my Gearman::Task $task = shift;
    return unless $task->{on_status};
    my ($nu, $de) = @_;
    $task->{on_status}->($nu, $de);
}

sub handle {
    my Gearman::Task $task = shift;
    return $task->{handle} unless @_;
    return $task->{handle} = shift;
}

package Gearman::Taskset;

sub new {
    my $class = shift;
    my Gearman::Client $client = shift;

    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{waiting} = {};
    $self->{need_handle} = [];
    $self->{client} = $client;

    ($self->{sockaddr}, $self->{sock}) = $client->_get_random_js_sock;
    return undef unless $self->{sock};

    return $self;
}

sub wait {
    my Gearman::Taskset $ts = shift;

    while (keys %{$ts->{waiting}}) {
        $ts->_process_packet();
    }
}

# ->add_task($func, <$scalar | $scalarref>, <$uniq | $opts_hashref>
#      opts:
#        -- uniq
#        -- on_complete
#        -- on_fail
#        -- on_status
#        -- retry_count
#        -- fail_after_idle
#        -- high_priority
# ->add_task(Gearman::Task)
#

sub add_task {
    my Gearman::Taskset $ts = shift;
    my $task;

    if (ref $_[0]) {
        $task = shift;
    } else {
        my $func = shift;
        my $arg_p = shift;   # scalar or scalarref
        my $opts = shift;    # $uniq or hashref of opts

        my $argref = ref $arg_p ? $arg_p : \$arg_p;
        unless (ref $opts eq "HASH") {
            $opts = { uniq => $opts };
        }

        $task = Gearman::Task->new($ts, $func, $argref, $opts);
    }

    my $req = Gearman::Util::pack_req_command("submit_job",
                                              ${ $task->submit_job_args_ref });
    my $len = length($req);
    my $rv = $ts->{sock}->syswrite($req, $len);
    die "Wrote $rv but expected to write $len" unless $rv == $len;

    push @{ $ts->{need_handle} }, $task;
    while (@{ $ts->{need_handle} }) {
        $ts->_process_packet;
    }

    return $task->handle;
}

sub _process_packet {
    my Gearman::Taskset $ts = shift;

    my $err;
    my $res = Gearman::Util::read_res_packet($ts->{sock}, \$err);
    return 0 unless $res;

    if ($res->{type} eq "job_created") {
        my Gearman::Task $task = shift @{ $ts->{need_handle} } or
            die "Um, got an unexpeted job_created notification";

        my $handle = ${ $res->{'blobref'} };
        $task->handle($handle);
        $ts->{waiting}{$handle} = $task;
        return;
    }

    if ($res->{type} eq "work_fail") {
        my $handle = ${ $res->{'blobref'} };
        my Gearman::Task $task = $ts->{waiting}{$handle} or
            die "Uhhhh:  got work_fail for unknown handle: $handle\n";

        delete $ts->{waiting}{$handle};
        $task->fail;
        return;
    }

    if ($res->{type} eq "work_complete") {
        ${ $res->{'blobref'} } =~ s/^(.+?)\0//
            or die "Bogus work_complete from server";
        my $handle = $1;
        my Gearman::Task $task = $ts->{waiting}{$handle} or
            die "Uhhhh:  got work_complete for unknown handle: $handle\n";

        $task->complete($res->{'blobref'});
        delete $ts->{waiting}{$handle};
        return;
    }

    if ($res->{type} eq "work_status") {
        my ($handle, $nu, $de) = split(/\0/, ${ $res->{'blobref'} });
        my Gearman::Task $task = $ts->{waiting}{$handle} or
            die "Uhhhh:  got work_status for unknown handle: $handle\n";

        $task->status($nu, $de);
        return;
    }

    die "Unknown/unimplemented packet type: $res->{type}";

}

package Gearman::Client;

sub new {
    my ($class, %opts) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{job_servers} = [];
    $self->{js_count} = 0;

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

sub dispatch_background {
    my Gearman::Client $self = shift;
    my ($func, $arg_p, $uniq) = @_;
    my $argref = ref $arg_p ? $arg_p : \$arg_p;
    Carp::croak("Function argument must be scalar or scalarref")
        unless ref $argref eq "SCALAR";

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

sub _get_js_sock {
    my $hostport = shift;
    # TODO: cache, and verify with ->connected
    my $sock = IO::Socket::INET->new(PeerAddr => $hostport,
                                 Timeout => 1)
        or return undef;
    $sock->autoflush(1);
    return $sock;
}

sub _get_random_js_sock {
    my Gearman::Client $self = shift;
    return undef unless $self->{js_count};

    my $ridx = int(rand($self->{js_count}));
    for (my $try = 0; $try < $self->{js_count}; $try++) {
        my $aidx = ($ridx + $try) % $self->{js_count};
        my $hostport = $self->{job_servers}[$aidx];
        my $sock = _get_js_sock($hostport)
            or next;
        return ($hostport, $sock);
    }
    return ();
}

sub get_status {
    my Gearman::Client $self = shift;
    my $handle = shift;
    my ($hostport, $shandle) = split(m!//!, $handle);
    print "  hostport=[$hostport], shandle=[$shandle]\n";
    return undef unless grep { $hostport eq $_ } @{ $self->{job_servers} };

    my $sock = _get_js_sock($hostport)
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
    return Gearman::JobStatus->new(@args);
}

1;
