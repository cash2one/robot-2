#!/bin/sh

function mkdir_digits_and_lowercase() {
    mkdir -p $1 && cd $1
    mkdir -p 0 1 2 3 4 5 6 7 8 9 a b c d e f g h i j k l m n o p q r s t u v w x y z
}

mkdir -p log
mkdir_digits_and_lowercase homepage.d/
mkdir_digits_and_lowercase www/

exit

For prefix
