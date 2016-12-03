package Gearman::Objects;
use version ();
$Gearman::Objects::VERSION = version->declare("2.002.002"); #TRIAL

use strict;
use warnings;

=head1 NAME

Gearman::Objects - a parent class for L<Gearman::Client> and L<Gearman::Worker>

=head1 METHODS

=cut

use constant DEFAULT_PORT => 4730;

use Carp            ();
use IO::Socket::IP  ();
use IO::Socket::SSL ();
use Socket          ();
use List::MoreUtils qw/
    first_index
    /;
use Ref::Util qw/
    is_plain_arrayref
    is_plain_hashref
    is_plain_ref
    is_ref
    /;

use fields qw/
    debug
    job_servers
    js_count
    prefix
    sock_cache
    /;

sub new {
    my $self = shift;
    my (%opts) = @_;
    unless (is_ref($self)) {
        $self = fields::new($self);
    }
    $self->{job_servers} = [];
    $self->{js_count}    = 0;

    $opts{job_servers}
        && $self->set_job_servers($opts{job_servers});

    $self->debug($opts{debug});
    $self->prefix($opts{prefix});

    $self->{sock_cache} = {};

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

    $self->{js_count} = scalar @{$list};
    return $self->{job_servers} = $list;
} ## end sub set_job_servers

=head2 canonicalize_job_servers($js)

C<$js> a string, hash reference or array reference of aforementioned.

Hash reference should contain at least host key.

All keys: host, port (4730 on default), use_ssl, key_file, cert_file,
ca_certs, socket_cb

B<return> [canonicalized list]

=cut

sub canonicalize_job_servers {
    my ($self) = shift;
    my @in;
    if (is_plain_ref($_[0])) {
        if (is_plain_arrayref($_[0])) {
            @in = @{ $_[0] };
        }
        elsif (is_plain_hashref($_[0])) {
            @in = ($_[0]);
        }
        else {
            Carp::croak "unsupported argument type ", ref($_[0]);
        }
    } ## end if (is_plain_ref($_[0]...))
    else {
        @in = @_;
    }

    my $out = [];
    foreach my $i (@in) {
        if (is_ref($i)) {
            $i->{port} ||= Gearman::Objects::DEFAULT_PORT;
        }
        elsif ($i !~ /:/) {
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

=head2 socket($js, [$timeout])

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
    my ($self, $js, $t) = @_;
    unless (is_ref($js)) {
        my ($h, $p) = ($js =~ /^(.*):(\d+)$/);
        $js = { host => $h, port => $p };
    }

    my %opts = (
        PeerPort => $js->{port},
        PeerHost => $js->{host},
        Timeout  => $t || 1
    );

    my $sc = "IO::Socket::IP";
    if ($js->{use_ssl}) {
        $sc = "IO::Socket::SSL";
        for (qw/ key_file cert_file ca_certs /) {
            $js->{$_} || next;
            $opts{ join('_', "SSL", $_) } = $js->{$_};
        }
    } ## end if ($js->{use_ssl})

    $js->{socket_cb} && $js->{socket_cb}->(\%opts);

    my $s = $sc->new(%opts);
    unless ($s) {
        $self->debug() && Carp::carp("connection failed error='$@'",
            $js->{use_ssl}
            ? ", ssl_error='$IO::Socket::SSL::SSL_ERROR'"
            : "");
    } ## end unless ($s)

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

# _sock_cache($js, [$sock, $delete])
#
# B<return> $sock || undef
#

sub _sock_cache {
    my ($self, $js, $sock, $delete) = @_;
    my $hp = $self->_js_str($js);
    if ($sock) {
        $self->{sock_cache}->{$hp} = $sock;
    }

    return $delete
        ? delete($self->{sock_cache}->{$hp})
        : $self->{sock_cache}->{$hp};
} ## end sub _sock_cache

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

#
#_js_str($js)
#
# return host:port
sub _js_str {
    my ($self, $js) = @_;
    return is_plain_hashref($js) ? join(':', @{$js}{qw/host port/}) : $js;
}

#
# _js($js_str)
#
# return job_servers item || undef
#
sub _js {
    my ($self, $js_str) = @_;
    my @s = $self->job_servers();
    my $i = first_index { $js_str eq $self->_js_str($_) } @s;
    return ($i == -1 || $i > $#s) ? undef : $s[$i];
} ## end sub _js

1;
