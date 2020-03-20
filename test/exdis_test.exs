defmodule ExdisTest do
  use ExUnit.Case
  doctest Exdis

  test "greets the world" do
    assert Exdis.hello() == :world
  end
end
