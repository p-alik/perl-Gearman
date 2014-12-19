use 5.008;
use strict;
use warnings;

use ExtUtils::MakeMaker;

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'         => 'Gearman',
    'VERSION_FROM' => 'lib/Gearman/Client.pm',
    'PREREQ_PM'    => {
        'fields'           => 0,
        'Carp'             => 0,
        'Errno'            => 0,
        'IO::Handle'       => 0,
        'IO::Socket::INET' => 0,
        'String::CRC32'    => 0,
        'Time::HiRes'      => 0,    # Usually core now
        'Scalar::Util'     => 0,
        'Socket'           => 0,
        'Storable'         => 1,
    },    # e.g., Module::Name => 1.1
    AUTHOR => 'Brad Fitzpatrick <brad@danga.com>',
    ABSTRACT =>
        "Client and worker libraries for gearman job dispatch dispatch.  Server is in separate package.",
);

1;
