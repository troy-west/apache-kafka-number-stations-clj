# Getting started



A web server at localhost:8080 can be started with:

    lein do clean, run

or, by compiling the namespace:

    number-stations.system

and running:

    (number-stations.system/start)

Tests can be found in namespace:

    number-stations.topology-test:

    - translate-numbers-test                 (translates ["one" "two" "three"] into 123)
    - correlate-rgb-test                     (groups 10 second windowed messages into rgb's)
    - number-stations-to-image-topology-test (generates an image)

# ..Archived..

... ignore the remainder ..

# Number Stations Generator

Generate a stream of numbers from an image to be used in a Kafka workshop as a decoding puzzle.

## Dimensionality

This is really a problem of dimensionality.

How do the different dimensions of an image and of number stations map between each other?

How do we exploit the mappings to create interest challenges for decoding the stream using Kafka?

I thought I would start by cataloging some of the various dimensions available:

### Image Dimensionality

* Spatial dimensions, height and width
* Colour dimensions, colour channels r, g, b and alpha
  - Hue, Saturation
* Number of pixels
* Colour run lengths (uninterupted rows of a single colour)
* Image meta data encoded in the image

### Number Station Dimensionality

[https://en.wikipedia.org/wiki/Numbers_station](https://en.wikipedia.org/wiki/Numbers_station)

* Spatial dimensions, lat, lon, coordinate reference
* Station name
  - Classified by language (English, German, Slavic, Other, Morse Code)
* Timezone
* Sequence of numbers
  - Timestamps
  - Prelude (with station identifer)
  - Number groups announcement
  - The numbers
  - Signoff (terminator word or sequence)

## Kakfa Streams tools

### Stateless

* map
* filter
* flatMap
* branch
* foreach
* groupBy
* groupByKey
* selectKey
* merge

### Stateful

#### Aggregating

* aggregate
* count
* reduce

#### Joining

* inner join streams/ktables
* left join streams/ktables
* outer join streams/ktables

#### Windowing

* tumbling
* hopping
* sliding
* session

#### PAPI

* ktables
* dedupe
