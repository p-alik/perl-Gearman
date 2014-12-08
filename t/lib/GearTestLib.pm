package GearTestLib;
use strict;
use IO::Socket::INET;
use Exporter 'import';
use FindBin;
use Carp qw(croak);
use vars qw(@EXPORT);

@EXPORT = qw(sleep);

sub sleep {
    my $n = shift;
    select undef, undef, undef, $n;
}

sub free_port {
    my $port = shift;
    my $type = shift || "tcp";
    my $sock;
    while (!$sock) {
        $sock = IO::Socket::INET->new(LocalAddr => '127.0.0.1',
                                      LocalPort => $port,
                                      Proto     => $type,
                                      ReuseAddr => 1);
        return $port if $sock;
        $port = int(rand(20000)) + 30000;
    }
    return $port;
}

sub start_child {
    my($cmd) = @_;
    my $pid = fork();
    die $! unless defined $pid;
    unless ($pid) {
        exec 'perl', '-Iblib/lib', '-Ilib', @$cmd or die $!;
    }
    $pid;
}

package Test::GearServer;
use List::Util qw(first);

my $requested_port = 8999;

sub new {
    my $class = shift;
    my $port = GearTestLib::free_port(++$requested_port);

    my @loc = ("$FindBin::Bin/../../../../server/gearmand",  # using svn
               '/usr/bin/gearmand',            # where some distros might put it
               '/usr/sbin/gearmand',           # where other distros might put it
               );
    my $server = first { -e $_ } @loc;
    unless ($server) {
        warn "Can't find gearmand in any of: @loc\n";
        return 0;
    }

    my $ready = 0;
    local $SIG{USR1} = sub {
        $ready = 1;
    };

    my $pid = GearTestLib::start_child([ $server, '-p' => $port, '-n' => $$ ]);
    while (!$ready) {
        select undef, undef, undef, 0.10;
    }
    return bless {
        pid => $pid,
        port => $port,
    }, $class;
}

sub ipport {
    my $self = shift;
    return "127.0.0.1:$self->{port}";
}

sub DESTROY {
    my $self = shift;
    kill 9, $self->{pid} if $self->{pid};
}

1;
