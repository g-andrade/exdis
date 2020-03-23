defmodule Exdis.Float do
  ## ------------------------------------------------------------------
  ## Constant Definitions
  ## ------------------------------------------------------------------

  # the DBL_MAX_10_EXP constant in `float.h`
  @max_decimal_string_length 20

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  @type t :: float

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def add(float, increment) do
    try do
      {:ok, float + increment}
    catch
      _, %ArithmeticError{} when is_float(float) and is_float(increment) ->
        {:error, :NaN_or_infinity}
    end
  end

  def decimal_string_length(integer) do
    byte_size( to_decimal_string(integer) )
  end

  def from_decimal_string(string) do
    case (
      byte_size(string) < @max_decimal_string_length
      and Float.parse(string))
    do
      {float, ""} ->
        {:ok, float}
      {_float, <<trailing_data :: bytes>>} ->
        {:error, {:trailing_data, trailing_data}}
      :error ->
        {:error, {:not_a_float, string}}
      false ->
        {:error, {:string_too_large, byte_size(string)}}
    end
  end

  def from_integer(integer) do
    try do
      case 0.0 + integer do
        float when float == integer ->
          {:ok, float}
        _ ->
          {:error, :loss_of_precision}
      end
    catch
      _, %ArithmeticError{} when is_integer(integer) ->
        {:error, :integer_is_too_large}
    end
  end

  def max_decimal_string_length(), do: @max_decimal_string_length

  def new(float) when is_float(float), do: float

  def to_decimal_string(float) do
    case round(float) do
      integer when integer == float ->
        Integer.to_string(integer)
      _ ->
        inspect float
    end
  end
end
