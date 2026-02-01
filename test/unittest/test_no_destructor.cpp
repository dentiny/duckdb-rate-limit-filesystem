#include "catch.hpp"
#include "no_destructor.hpp"

using namespace duckdb;

TEST_CASE("NoDestructor - default constructor", "[no_destructor]") {
	NoDestructor<string> content {};
	REQUIRE(*content == "");
}

TEST_CASE("NoDestructor - construct by const reference", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s};
	REQUIRE(*content == s);
}

TEST_CASE("NoDestructor - construct by rvalue reference", "[no_destructor]") {
	const string expected = "helloworld";
	string another_str = "helloworld";
	NoDestructor<string> content {std::move(another_str)};
	REQUIRE(*content == expected);
}

TEST_CASE("NoDestructor - construct by ctor with multiple arguments", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s.begin(), s.end()};
	REQUIRE(*content == "helloworld");
}

TEST_CASE("NoDestructor - access internal object", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s.begin(), s.end()};
	(*content)[0] = 'b';
	(*content)[1] = 'c';
	REQUIRE(*content == "bclloworld");
}

TEST_CASE("NoDestructor - reassign", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s.begin(), s.end()};
	*content = "worldhello";
	REQUIRE(*content == "worldhello");
}

TEST_CASE("NoDestructor - arrow operator", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s};
	REQUIRE(content->size() == 10);
	REQUIRE(content->empty() == false);
}

TEST_CASE("NoDestructor - get method", "[no_destructor]") {
	const string s = "helloworld";
	NoDestructor<string> content {s};
	string *ptr = content.get();
	REQUIRE(ptr != nullptr);
	REQUIRE(*ptr == s);
}

TEST_CASE("NoDestructor - const access", "[no_destructor]") {
	const string s = "helloworld";
	const NoDestructor<string> content {s};
	REQUIRE(*content == s);
	REQUIRE(content->size() == 10);
	REQUIRE(content.get() != nullptr);
}

TEST_CASE("NoDestructor - vector type", "[no_destructor]") {
	NoDestructor<vector<int>> nums {{1, 2, 3, 4, 5}};
	REQUIRE(nums->size() == 5);
	REQUIRE((*nums)[0] == 1);
	REQUIRE((*nums)[4] == 5);
}
