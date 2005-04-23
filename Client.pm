#!/usr/bin/perl

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
sub new {
    my $class = shift;
    my Gearman::Client $client = shift;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{waiting} = {};
    $self->{need_handle} = [];
    $self->{client} = $client;

    ($self->{sockaddr}, $self->{sock}) = $client->_get_random_js_sock;

    return $self;
}

sub wait {
    my Gearman::Taskset $ts = shift;

    while (@{$ts->{need_handle}} || keys %{$ts->{waiting}}) {
        print "Waiting for packet.\n";
        $ts->_process_packet();
    }
}

sub add_task {
    my Gearman::Taskset $ts = shift;
    my $func = shift;
    my $arg_p = shift;   # scalar or scalarref
    my $opts = shift;    # $uniq or hashref of opts

    my $argref = ref $arg_p ? $arg_p : \$arg_p;
    unless (ref $opts eq "HASH") {
        $opts = { uniq => $opts };
    }

    my $req = Gearman::Util::pack_req_command("submit_job",
                                              join("\0",
                                                   $func,
                                                   $opts->{uniq},
                                                   $$arg_p));
    my $len = length($req);
    my $rv = $jss->write($req, $len);
    die unless $rv == $len;

    my $task = [ $func, $argref, $opts, undef ];
    push @{ $ts->{need_handle} }, $task;
    while (@{ $ts->{need_handle} }) {
        print "Waiting for handle packet.\n";
        $ts->_process_packet;
    }
    return $task->[3];
}

sub _process_packet {
    my Gearman::Taskset $ts = shift;

    my $err;
    my $res = Gearman::Util::read_res_packet($ts->{sock}, \$err);
    return 0 unless $res;

    if ($res->{type} eq "job_created") {
        my $job = shift @{ $ts->{need_handle} };
        die "Um, got an unexpeted job_created notification" unless $job;
        my $handle = ${ $res->{'blobref'} };
        $job->[3] = $handle;
        $ts->{waiting}{$handle} = $job;
        return;
    }

    #TODO: fails
    #TODO: completes
    #TODO: status

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

    my ($jst, $jss) = $self->_get_random_js_sock
        or return 0;

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
