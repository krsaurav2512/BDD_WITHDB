@practice
Feature: Practice test

  @test_practice
  Scenario Outline: My test 1
    Given user is on flipkart login page
    When user enters "<USER_NAME>" and "<PASSWORD>", click on login btn and verifies login staus

    Examples: 
      | USER_NAME | PASSWORD | STATUS |
      |           |          |        |
