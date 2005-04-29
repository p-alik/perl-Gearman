#!/usr/bin/perl

#TODO: TCP_NODELAY
#TODO: retries

use strict;
use Gearman::Util;
use Carp ();
use IO::Socket::INET;


package Gearman::Job;

use fields (
            'func',
            'argref',
            'handle',
            );

sub new {
    my ($class, $func, $argref, $handle) = @_;
    my $self = $class;
    $self = fields::new($class) unless ref $self;

    $self->{func} = $func;
    $self->{handle} = $handle;
    $self->{argref} = $argref;
    return $self;
}

sub set_status {
    my Gearman::Job $self = shift;
    my ($nu, $de) = @_;
    print "status of $self->{handle}: $nu/$de\n";
    # TODO: send to jobserver
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
use fields (
            'job_servers',
            'js_count',
            'sock_cache',        # host:port -> IO::Socket::INET
            'last_connect_fail', # host:port -> unixtime
            'down_since',        # host:port -> unixtime
            'can',               # func -> subref
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

    $self->job_servers(@{ $opts{job_servers} })
        if $opts{job_servers};

    return $self;
}

sub _get_js_sock {
    my Gearman::Worker $self = shift;
    my $ip = shift;

    if (my $sock = $self->{sock_cache}{$ip}) {
        return $sock if $sock->connected;
        delete $self->{sock_cache}{$ip};
    }

    my $now = time;
    my $down_since = $self->{down_since}{$ip};
    if ($down_since) {
        my $down_for = $now - $down_since;
        my $retry_period = $down_for > 60 ? 30 : (int($down_for / 2) + 1);
        if ($self->{last_connect_fail}{$ip} > $now - $retry_period) {
            return undef;
        }
    }

    my $sock = IO::Socket::INET->new(PeerAddr => $ip,
                                     Timeout => 1);
    unless ($sock) {
        $self->{down_since}{$ip} ||= $now;
        $self->{last_connect_fail}{$ip} = $now;
        return undef;
    }
    delete $self->{last_connect_fail}{$ip};
    delete $self->{down_since}{$ip};
    $sock->autoflush(1);

    $self->{sock_cache}{$ip} = $sock;

    # get this socket's state caught-up
    foreach my $func (keys %{$self->{can}}) {
        unless (_set_capability($sock, $func, 1)) {
            delete $self->{sock_cache}{$ip};
            return undef;
        }
    }

    return $sock;
}

sub _send_req {
    my ($sock, $reqref) = @_;

    my $len = length($$reqref);
    #TODO: catch SIGPIPE
    my $rv = $sock->syswrite($$reqref, $len);
    return 0 unless $rv == $len;
    return 1;
}

sub _set_capability {
    my ($sock, $func, $can) = @_;

    my $req = Gearman::Util::pack_req_command($can ? "can_do" : "cant_do",
                                              $func);
    return _send_req($sock, \$req);
}

# tell all the jobservers that this worker can't do anything
sub reset_abilities {
    my Gearman::Worker $self = shift;
    my $req = Gearman::Util::pack_req_command("reset_abilities");
    foreach my $js (@{ $self->{job_servers} }) {
        my $jss = $self->_get_js_sock($js);
        unless (_send_req($jss, \$req)) {
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

    while (1) {

        my @jss;
        my $need_sleep = 1;

        foreach my $js (@{ $self->{job_servers} }) {
            my $jss = $self->_get_js_sock($js);
            unless (_send_req($jss, \$grab_req) &&
                    Gearman::Util::wait_for_readability($jss->fileno, 0.25)) {
                delete $self->{sock_cache}{$js};
                next;
            }
            push @jss, [$js, $jss];

            my $err;
            my $res = Gearman::Util::read_res_packet($jss, \$err);
            next unless $res;

            if ($res->{type} eq "no_job") {
                next;
            }

            # if we get a noop packet, we should do another pass quickly to
            # ask for the job, because they probably have one for us now.
            elsif ($res->{type} eq "noop") {
                $need_sleep = 0;
                next;
            }

            die "Uh, wasn't expecting a $res->{type} packet" unless $res->{type} eq "job_assign";

            ${ $res->{'blobref'} } =~ s/^(.+?)\0(.+?)\0//
                or die "Uh, regexp on job_assign failed";
            my ($handle, $func) = ($1, $2);
            my $job = Gearman::Job->new($func, $res->{'blobref'}, $handle);
            my $handler = $self->{can}{$func};
            my $ret = eval { $handler->($job); };
            print "For func: $func, handler=$handler, ret=$ret: errors=[$@]\n";
            my $work_req;
            if (defined $ret) {
                $work_req = Gearman::Util::pack_req_command("work_complete", "$handle\0" . (ref $ret ? $$ret : $ret));
            } else {
                $work_req = Gearman::Util::pack_req_command("work_fail", $handle);
            }

            unless (_send_req($jss, \$work_req)) {
                delete $self->{sock_cache}{$js};
            }
            return;
        }

        if ($need_sleep) {
            my $wake_vec;
            foreach my $j (@jss) {
                my ($js, $jss) = @$j;
                unless (_send_req($jss, \$presleep_req)) {
                    delete $self->{sock_cache}{$js};
                    next;
                }
                vec($wake_vec, $jss->fileno, 1) = 1;
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
        unless (_send_req($jss, \$req)) {
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
