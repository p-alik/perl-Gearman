package Test::Gearman;
use base qw(Exporter);
@EXPORT = qw(
    start_worker
    respawn_children
    pid_is_dead
    %Children
);

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
use POSIX qw( :sys_wait_h );

use FindBin qw( $Bin );

our %Children;

END { kill_children() }

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
        my $pid = start_server($self->{daemon}, $_, $self->is_perl_daemon());
        unless ($pid) {
            $ok = 0;
            last;
        }

        push @{ $self->{_job_servers} }, join ':', $self->{ip}, $_;
        $Children{$pid} = 'S';
    } ## end foreach (@{ $self->{ports} ...})
    return $ok;
} ## end sub start_servers

sub start_server {
    my ($daemon, $port, $is_perl_daemon) = @_;
    my $pid;
    unless ($is_perl_daemon) {
        $pid = start_child("$daemon -p $port -d  -l /dev/null", 1);
    }
    else {
        my $ready = 0;
        local $SIG{USR1} = sub {
            $ready = 1;
        };
        $pid = start_child([$daemon, '-p' => $port, '-n' => $$]);
        while (!$ready) {
            select undef, undef, undef, 0.10;
        }
    } ## end else

    return $pid;
} ## end sub start_server

sub start_worker {
    my ($job_servers, $args) = @_;
    unless (ref $args) {
        $args = {};
    }

    my $worker = "$Bin/worker.pl";
    warn $worker;
    my $servers = join ',', @{$job_servers};
    my $ready = 0;
    my $pid;
    local $SIG{USR1} = sub {
        $ready = 1;
    };
    $pid = start_child(
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

sub start_child {
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
} ## end sub start_child

sub kill_children {
    kill INT => keys %Children;
}

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
    my ($pid) = shift;
    return if $pid == -1;
    my $type = delete $Children{$pid};
    if ($type eq 'W') {
        ## Right now we can only restart workers.
        start_worker(@_);
    }
} ## end sub pid_is_dead

sub respawn_children {
    for my $pid (keys %Children) {
        if (waitpid($pid, WNOHANG) > 0) {
            pid_is_dead($pid, @_);
        }
    }
} ## end sub respawn_children

1;
