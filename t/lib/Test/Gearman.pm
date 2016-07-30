package Test::Gearman;
use base qw(Exporter);

use strict;
use warnings;

use fields qw/
    daemon
    ports
    ip
    count
    _is_perl_daemon
    _job_servers
    /;

use IO::Socket::INET;
use POSIX qw/ :sys_wait_h /;

use FindBin qw/ $Bin /;

my %Children;

END {
    foreach (keys %Children) {
        if ($Children{$_} ne 'W' && $Children{$_} ne 'S') {
            qx/kill `cat $Children{$_}`/;
        }
        else {
            kill INT => $_;
        }
    } ## end foreach (keys %Children)
} ## end END

sub new {
    my ($class, %args) = @_;
    my $self = fields::new($class);

    $self->{daemon} = $args{daemon} || qx/which gearmand/;
    chomp $self->{daemon};

    $self->{ports} = $self->_free_ports($args{count});
    $self->{ip}    = $args{ip};

    return $self;
} ## end sub new

sub is_perl_daemon {
    my ($self) = @_;
    $self->{daemon} || return;

    unless (defined $self->{_is_perl_daemon}) {
        my $v = qx/$self->{daemon} -V/;
        $self->{_is_perl_daemon} = ($v && $v =~ /Gearman::Server/);
    }
    return $self->{_is_perl_daemon};
} ## end sub is_perl_daemon

sub _free_ports {
    my ($self, $count) = @_;
    $count ||= 1;
    my @p;
    for (1 .. $count) {
        my $fp = _free_port($self->{ip});
        $fp && push @p, $fp;
    }

    unless (scalar(@p) == $count) {
        warn "couldn't find $count free ports";
        return;
    }
    return [@p];
} ## end sub _free_ports

sub _free_port {
    my ($la, $port) = shift;
    my ($type, $retry, $sock) = ("tcp", 5);
    do {
        unless ($port) {
            $port = int(rand(20000)) + 30000;
        }

        IO::Socket::INET->new(
            LocalAddr => $la,
            LocalPort => $port,
            Proto     => $type,
            ReuseAddr => 1
        ) or undef($port);

    } until ($port || --$retry == 0);

    return $port;
} ## end sub _free_port

sub job_servers {
    return shift->{_job_servers};
}

sub start_servers {
    my ($self) = @_;
    ($self->{daemon} && $self->{ports}) || return;
    (-e $self->{daemon}) || return;

    my $ok = 1;
    foreach (@{ $self->{ports} }) {
        my $pid = $self->_start_server($_);
        unless ($pid) {
            $ok = 0;
            last;
        }

        push @{ $self->{_job_servers} }, join ':', $self->{ip}, $_;
        $Children{$pid}
            = $self->is_perl_daemon() ? 'S' : $self->_pid_file("daemon", $_);
    } ## end foreach (@{ $self->{ports} ...})

    return $ok;
} ## end sub start_servers

sub _pid_file {
    my ($self) = shift;
    return join '/', "/tmp", join('-', @_);
}

sub _start_server {
    my ($self, $port) = @_;
    my $pid;

    my $daemon = $self->{daemon};

    my $pf = $self->_pid_file("daemon", $port);
    unless ($self->is_perl_daemon()) {
        my ($verbose, $lf) = ('');
        if ($ENV{DEBUG}) {
            $lf = join('.', $pf, "log");
            $verbose = "--verbose=INFO";
        }
        else {
            $lf = "/dev/null";
        }
        $pid
            = _start_child("$daemon -p $port -d -P $pf --log-file=$lf $verbose",
            1);
    } ## end unless ($self->is_perl_daemon...)
    else {
        my $ready = 0;
        local $SIG{USR1} = sub {
            $ready = 1;
        };
        $pid = _start_child([$daemon, '-p' => $port, '-n' => $$]);
        while (!$ready) {
            select undef, undef, undef, 0.10;
        }
    } ## end else

    return $pid;
} ## end sub _start_server

sub start_worker {
    my ($self, $args) = @_;
    $self->job_servers || die "no running job servers";
    unless (ref $args) {
        $args = {};
    }

    my $worker  = "$Bin/worker.pl";
    my $servers = join ',', @{ $self->job_servers };
    my $ready   = 0;
    my $pid;
    local $SIG{USR1} = sub {
        $ready = 1;
    };
    $pid = _start_child(
        [
            $worker,
            '-s' => $servers,
            '-n' => $$,
            ($args->{prefix} ? ('-p' => $args->{prefix}) : ())
        ]
    );
    $Children{$pid} = 'W';
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return $pid;
} ## end sub start_worker

sub check_server_connection {
    my ($self, $pa) = @_;
    my ($start, $sock, $to) = (time);
    do {
        $sock = IO::Socket::INET->new(PeerAddr => $pa);
        select undef, undef, undef, 0.25;
        $to = time > $start + 5;
    } until ($sock || $to);

    $to && warn "Timeout waiting for peer address $pa";

    return (defined($sock) && !$to);
} ## end sub check_server_connection

sub pid_is_dead {
    my ($self, $pid) = @_;
    return if $pid == -1;
    if (delete $Children{$pid} eq 'W') {
        ## Right now we can only restart workers.
        $self->start_worker();
    }
} ## end sub pid_is_dead

sub respawn_children {
    my ($self) = @_;
    for my $pid (keys %Children) {
        $Children{$pid} eq 'W' || next;
        if (waitpid($pid, WNOHANG) > 0) {
            $self->pid_is_dead($pid);
        }
    } ## end for my $pid (keys %Children)
} ## end sub respawn_children

sub stop_worker {
    my ($self, $pid) = @_;
    ($Children{$pid} && $Children{$pid} eq 'W') || return;
    kill INT => ($pid);
}

sub _start_child {
    my ($cmd, $binary) = @_;
    my $pid = fork();
    die $! unless defined $pid;
    unless ($pid) {
        if (!$binary) {
            exec $^X, '-Iblib/lib', '-Ilib', @$cmd or die $!;
        }
        else {
            exec($cmd) or die $!;
        }
    } ## end unless ($pid)
    $pid;
} ## end sub _start_child

1;
