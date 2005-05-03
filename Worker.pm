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
    print "status of $self->{handle}: $nu/$de\n";

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

    my $sock;
    socket $sock, PF_INET, SOCK_STREAM, IPPROTO_TCP;
    #IO::Handle::blocking($sock, 0);
    connect $sock, Socket::sockaddr_in($port, Socket::inet_aton($ip));

    #my $sock = IO::Socket::INET->new(PeerAddr => $ip,
    #                                 Timeout => 1);
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
        my $jss = $self->_get_js_sock($js);
        unless (Gearman::Util::send_req($jss, \$req)) {
            delete $self->{sock_cache}{$js};
        }
    }

    $self->{can} = {};
}

# does one job and returns.  no return value.
sub work {
    my Gearman::Worker $self = shift;
    my $grab_req = Gearman::Util::pack_req_command("grab_job");
    my $presleep_req = Gearman::Util::pack_req_command("pre_sleep");
    my %fd_map;

    while (1) {

        my @jss;
        my $need_sleep = 1;

        foreach my $js (@{ $self->{job_servers} }) {
            my $jss = $self->_get_js_sock($js)
                or next;

            unless (Gearman::Util::send_req($jss, \$grab_req) &&
                    Gearman::Util::wait_for_readability($jss->fileno, 0.50)) {
                delete $self->{sock_cache}{$js};
                next;
            }
            push @jss, [$js, $jss];

            my ($res, $err);
            do {
                $res = Gearman::Util::read_res_packet($jss, \$err);
            } while ($res && $res->{type} eq "noop");

            next unless $res;

            if ($res->{type} eq "no_job") {
                next;
            }

            die "Uh, wasn't expecting a $res->{type} packet" unless $res->{type} eq "job_assign";

            ${ $res->{'blobref'} } =~ s/^(.+?)\0(.+?)\0//
                or die "Uh, regexp on job_assign failed";
            my ($handle, $func) = ($1, $2);
            my $job = Gearman::Job->new($func, $res->{'blobref'}, $handle, $jss);
            my $handler = $self->{can}{$func};
            my $ret = eval { $handler->($job); };
            print "For func: $func, handler=$handler, ret=$ret: errors=[$@]\n";
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
            my $wake_vec = undef;
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
    }

}

sub register_function {
    my Gearman::Worker $self = shift;
    my $func = shift;
    my $subref = shift;

    my $req = Gearman::Util::pack_req_command("can_do", $func);

    foreach my $js (@{ $self->{job_servers} }) {
        my $jss = $self->_get_js_sock($js);
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
