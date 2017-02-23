#!/usr/bin/env bash
info() {
  echo -e "\e[32mo $1\e[0m"
}

error() {
  echo -e "\e[31mx $1\e[0m"
}

is_install() {
  check=$(which $1 ; echo $?)
  echo $check;
}