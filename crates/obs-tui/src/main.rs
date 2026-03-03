#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Placeholder,
}

impl Route {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Placeholder => "placeholder",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppState {
    route: Route,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            route: Route::Placeholder,
        }
    }
}

fn main() {
    let app = AppState::default();
    println!("obs-tui started route={}", app.route.as_str());
}

#[cfg(test)]
mod tests {
    use super::{AppState, Route};

    #[test]
    fn default_route_is_placeholder() {
        let app = AppState::default();
        assert_eq!(app.route, Route::Placeholder);
        assert_eq!(app.route.as_str(), "placeholder");
    }
}
