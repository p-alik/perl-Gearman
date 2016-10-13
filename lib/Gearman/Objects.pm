package Gearman::Objects;
use version;
$Gearman::Objects::VERSION = qv("2.001.001"); # TRIAL

use strict;
use warnings;

=head1 NAME

Gearman::Objects - a parrent class for L<Gearman::Client> and L<Gearman::Worker>

=head1 METHODS

=cut

use constant DEFAULT_PORT => 4730;

use Carp            ();
use IO::Socket::IP  ();
use IO::Socket::SSL ();
use Socket          ();

use fields qw/
    debug
    job_servers
    js_count
    prefix
    use_ssl
    ssl_socket_cb
    /;

sub new {
    my Gearman::Objects $self = shift;
    my (%opts) = @_;
    unless (ref($self)) {
        $self = fields::new($self);
    }
    $self->{job_servers} = [];
    $self->{js_count}    = 0;

    $opts{job_servers}
        && $self->set_job_servers(
        ref($opts{job_servers})
        ? @{ $opts{job_servers} }
        : [$opts{job_servers}]
        );

    $self->debug($opts{debug});
    $self->prefix($opts{prefix});
    if ($self->use_ssl($opts{use_ssl})) {
        $self->{ssl_socket_cb} = $opts{ssl_socket_cb};
    }

    return $self;
} ## end sub new

=head2 job_servers([$js])

getter/setter

C<$js> array reference or scalar

=cut

sub job_servers {
    my ($self) = shift;
    (@_) && $self->set_job_servers(@_);

    return wantarray ? @{ $self->{job_servers} } : $self->{job_servers};
} ## end sub job_servers

=head2 set_job_servers($js)

set job_servers attribute by canonicalized C<$js>_

=cut

sub set_job_servers {
    my $self = shift;
    my $list = $self->canonicalize_job_servers(@_);

    $self->{js_count} = scalar @$list;
    return $self->{job_servers} = $list;
} ## end sub set_job_servers

=head2 canonicalize_job_servers($js)

C<$js> array reference or scalar

B<return> [canonicalized list]

=cut

sub canonicalize_job_servers {
    my ($self) = shift;
    my @in;

    # take arrayref or array
    if (ref($_[0])) {
        ref($_[0]) eq "ARRAY"
            || Carp::croak
            "canonicalize_job_servers argument is not a reference on array";
        @in = @{ $_[0] };
    } ## end if (ref($_[0]))
    else {
        @in = @_;
    }

    my $out = [];
    foreach my $i (@in) {
        $i
            || Carp::croak
            "canonicalize_job_servers argument contails an undefined parameter";
        if ($i !~ /:/) {
            $i .= ':' . Gearman::Objects::DEFAULT_PORT;
        }
        push @{$out}, $i;
    } ## end foreach my $i (@in)
    return $out;
} ## end sub canonicalize_job_servers

sub debug {
    return shift->_property("debug", @_);
}

=head2 prefix([$prefix])

getter/setter

=cut

sub prefix {
    return shift->_property("prefix", @_);
}

sub use_ssl {
    return shift->_property("use_ssl", @_);
}

=head2 socket($host_port, [$timeout])

depends on C<use_ssl> 
prepare L<IO::Socket::IP>
or L<IO::Socket::SSL>

=over

=item

C<$host_port> peer address

=item

C<$timeout> default: 1

=back

B<return> depends on C<use_ssl> IO::Socket::(IP|SSL) on success

=cut

sub socket {
    my ($self, $pa, $t) = @_;
    my ($h, $p) = ($pa =~ /^(.*):(\d+)$/);

    my %opts = (
        PeerPort => $p,
        PeerHost => $h,
        Timeout  => $t || 1
    );
    my $sc;
    if ($self->use_ssl()) {
        $sc = "IO::Socket::SSL";
        $self->{ssl_socket_cb} && $self->{ssl_socket_cb}->(\%opts);
    }
    else {
        $sc = "IO::Socket::IP";
    }

    my $s = $sc->new(%opts);
    $s || Carp::carp("connection failed error='$@'",
        $self->use_ssl()
        ? ", ssl_error='$IO::Socket::SSL::SSL_ERROR'"
        : "");

    return $s;
} ## end sub socket

=head2 sock_nodelay($sock)

set TCP_NODELAY on $sock, die on failure

=cut
sub sock_nodelay {
    my ($self, $sock) = @_;
    setsockopt($sock, Socket::IPPROTO_TCP, Socket::TCP_NODELAY, pack("l", 1))
        or Carp::croak "setsockopt: $!";
}

#
# _property($name, [$value])
# set/get
sub _property {
    my $self = shift;
    my $name = shift;
    $name || return;
    if (@_) {
        $self->{$name} = shift;
    }

    return $self->{$name};
} ## end sub _property

1;
